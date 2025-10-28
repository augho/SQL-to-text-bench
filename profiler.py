import queue

from src.lib.SqliteConnector import SqliteConnector
from src.lib.utils import (
    log,
    write_json,
    read_dir_files,
    human_in_the_loop,
    sqlite_export,
    create_dir_if_not_exists,
    run_rate_limited_tasks_with_retry
)
from src.profiling.GenAi import TableDescriptionOutput, TableDescription, field_desc_creation_str, table_desc_creation_str, GenAiApi, AiApiError
from src.profiling.GeminiApi import Gemini
from src.lib.Config import OutputFormat
from src.profiling.ProfilingConfig import ProfilingConfig
from src.profiling.Models import (
    DatabaseMetadata,
    LengthMetaData,
    TableMetadata,
    ColumnMetadata,
    ModelOutput
)


def table_profile(
    db: SqliteConnector, tablename: str, sample_size: int
) -> TableMetadata:
    row_count = db.table_row_count(tablename)

    cols = db.table_columns(tablename)

    columns_metadata = []
    for c in cols:
        column_name = c["column_name"]
        column_type = c["column_type"]
        allows_null = c["allows_null"]
        is_pk = c["is_pk"]
        null_count, non_null_count = db.count_nulls_and_nonnulls(tablename, column_name)
        distinct_count = db.distinct_count(tablename, column_name)

        # TODO do max/min only for integer and not for string
        if non_null_count > 0:
            mn, mx = None, None 
        else:
            mn, mx = db.min_max_for_column(tablename, column_name)

        min_len, avg_len, max_len = db.length_stats_sql(tablename, column_name)

        samples = []
        if non_null_count > 0 and sample_size > 0:
            samples = db.sample_values(tablename, column_name, sample_size=sample_size)
            # TODO maybe change this max preview to be configurable
            MAX_PREVIEW_SIZE = 5
            samples = samples[: min(MAX_PREVIEW_SIZE, len(samples) - 1)]

        col_metadata = ColumnMetadata(
            name=column_name,
            declared_type=column_type,
            allows_null=allows_null,
            is_pk=is_pk,
            null_count=null_count,
            non_null_count=non_null_count,
            distinct_count=distinct_count,
            min_value=mn,
            max_value=mx,
            length=LengthMetaData(min=min_len, average=avg_len, max=max_len),
            samples=samples,
        )
        columns_metadata.append(col_metadata)

    return TableMetadata(name=tablename, columns=columns_metadata, row_count=row_count)


def read_metadata_backup_folder(foldername: str) -> DatabaseMetadata | None:
    tables: list[TableMetadata] = []
    try:
    
        for filename in read_dir_files(foldername):
            if not filename.endswith(".json") or filename.endswith(".llm.json"):
                continue

            table = TableMetadata.from_file(foldername + "/" + filename)
            if table is None:
                raise FileNotFoundError(f"Failed to load table metadata for {filename}")
            tables.append(table)

        if len(tables) == 0:
            raise FileNotFoundError(
                "No metadata found in folder", ProfilingConfig.OUTPUT_PATH
            )

        return DatabaseMetadata(name=foldername, tables=tables)
    except FileNotFoundError as err:
        print(err)
        return None

def run_metadata_extraction() -> DatabaseMetadata:
    db = SqliteConnector(ProfilingConfig.DB_CONN_STRING)
    db_metadata = {}
    tables: list[TableMetadata] = []
    for tablename in db.list_tables():
        result = table_profile(db=db, tablename=tablename, sample_size=10)
        db_metadata[tablename] = result
        tables.append(result)

    return DatabaseMetadata(name=ProfilingConfig.DB_CONN_STRING, tables=tables)


# TODO fix this fn
def fix_generated_output(
    generation_result: dict[str, TableDescriptionOutput], db_metadata: DatabaseMetadata
) -> None:
    
    for tablename, model_output in generation_result.items():
        truth = db_metadata.find_table_by_name(tablename)
        assert truth is not None

        if not model_output.success:
            log(f"Model error: {model_output.error}")
            # add empty field desc when error so that its not empty
            model_output.data = TableDescription.from_metadata(truth)
        else:
            # Ensure model didn't hallucinate table or field name
            # Must have equal number of columns
            if len(model_output.data.columns) != len(truth.columns):
                model_output.success = False
                model_output.error = f"[ERR 1]Model hallucinated columns (expected {len(truth.columns)}, got {len(model_output.data.columns)})"
                model_output.data = TableDescription.from_metadata(truth)
                continue

            for field_metadata in model_output.data.columns:
                if truth.find_column_by_name(field_metadata.name) is None:
                    model_output.success = False
                    model_output.error = f"[ERR 2] Model hallucinated column name (hallucination={field_metadata})"
                    model_output.data = TableDescription.from_metadata(truth)
                    break
    


def run_metadata_llm_summary(
    db_metadata: DatabaseMetadata | None, report_filename: str, llm: GenAiApi
) -> None:
    # TODO add a custom folder
    # If no extraction we read the output folder for backed up file
    if not ProfilingConfig.DO_EXTRACTION:
        db_metadata = read_metadata_backup_folder(ProfilingConfig.OUTPUT_PATH)

    if db_metadata is None:
        log("Aborting llm summary")
        return

    generations_output: dict[str, ModelOutput] = {}

    # TODO Remove the filter
    run_args = [(table, generations_output) for table in db_metadata.tables if table.name == "Artist"]

    log("\n======= LLM RUN DETAILS =======")
    log(f"llm_model: {llm.get_model_name()}")
    log(f"# of input tables: {len(run_args)}")
    log(f"table_list: {[x[0].name for x in run_args]}")
    log(f"output_path: {ProfilingConfig.OUTPUT_PATH}/{report_filename}.llm.json")
    log(f"Max RPM: {ProfilingConfig.MAX_RPM}")
    log("========                 =======\n")

    # If no logging set we don't ask for confirmation
    proceed_confirmation = (
        True
        if ProfilingConfig.SKIP_INTERACTIONS
        else human_in_the_loop("Do you wish to continue (y/n)?")
    )

    if not proceed_confirmation:
        log("\n[WARN] Aborting llm table summarization !\n")
        return

    # TODO add a way to read from a saved output instead
    # ---------------------------------
    # Rate limited api calls
    error_queue = queue.Queue()
    def generation_cb(
        table_metadata: TableMetadata, output_dict: dict[str, ModelOutput]
    ):
        result, api_error = llm.summarize_table_metadata(table_metadata)
        if api_error is not None:
            error_queue.put((api_error, (table_metadata, output_dict)))

        # We still add the result which is an empty table but it will be overwritten if we retry   
        output_dict[table_metadata.name] = result

    # run_rate_limited_tasks(
    #     cb=generation_cb,
    #     cb_args=run_args
    # )
    run_rate_limited_tasks_with_retry(
        cb=generation_cb,
        cb_args=run_args,
        error_queue=error_queue,
        retry_limit=3,
        error_cb=llm.retry_strategy
    )


    
    # Ensures model didn't hallucinate table or field name 
    fix_generated_output(generations_output, db_metadata) 

    # generations_output = json_import("out/profiles/Chinook/ty_gemini.llm.json")

    create_dir_if_not_exists(ProfilingConfig.OUTPUT_PATH)
    export_success = export_model_outputs(
        generations_output,
        f"{ProfilingConfig.OUTPUT_PATH}/{report_filename}.llm",
        ProfilingConfig.OUTPUT_FORMAT,
    )

    

    success_count = 0
    error_count = 0
    for model_output in generations_output.values():
        if model_output.success:
            success_count += 1
        else:
            error_count += 1

    log("\n======= LLM RUN RESULT =======")
    log(f"# success: {success_count}")
    log(f"# errors: {error_count}")
    log(
        f"\n[LOG] Summary generated at {ProfilingConfig.OUTPUT_PATH}/{report_filename}.llm.json\n"
    )

    if not export_success:
        log("[ERR] Couldn't export your data")
        log(str({k: v.model_dump() for k, v in generations_output.items()}))


def export_model_outputs(
    model_outputs: dict[str, TableDescriptionOutput], filepath: str, format: OutputFormat
) -> bool:
    match format:
        case OutputFormat.JSON:
            return write_json(
                filepath + ".json", json_output={k: v.model_dump() for k, v in model_outputs.items()}
            )
        case OutputFormat.SQLITE:
            # (tablename, sql query to create table)
            schema: list[tuple[str, str]] = [
                ("table_description", table_desc_creation_str),
                ("field_description", field_desc_creation_str),
            ]
            sql_data: list[list[tuple]] = [[], []]

            field_count: int = 0
            for i, (tablename, model_output) in enumerate(model_outputs.items()):
                # Defining schema
                schema.append((tablename, table_desc_creation_str))

                # Defining tuples to be inserted
                # ---- TABLE DESCRIPTIONS
                # (id, name, desc)
                sql_data[0].append(
                    (
                        i,
                        tablename,
                        None if not model_output.success else model_output.data.table,
                    )
                )
                # ---- FIELD DESCRIPTIONS
                # (id, table_id, name, desc)
                fields = model_output.data.columns

                for f in fields:
                    sql_data[1].append((field_count, i, f.name, f.description))
                    field_count += 1

            return sqlite_export(sql_data, schema, filepath + ".sqlite")
        case _:
            raise ValueError(f"Output format unsupported: {format}")


def run(llm: GenAiApi):
    _profiling_config = ProfilingConfig.create_from_parser()
    ProfilingConfig.init(_profiling_config)

    log("============ Profiling config ================")
    log(str(_profiling_config.model_dump()))
    log("==============================================\n")

    extracted_metadata = None
    # ---------------------------------
    # Read metadata by querying database and save the metadata to output_path if requested
    if ProfilingConfig.DO_EXTRACTION:
        if ProfilingConfig.DRY_RUN:
            print(
                f"[DRY] run_metadata_extraction: reading metadata from {ProfilingConfig.DB_CONN_STRING}"
            )
        else:
            extracted_metadata = run_metadata_extraction()

        if ProfilingConfig.SAVE_METADATA:
            if ProfilingConfig.DRY_RUN:
                print(
                    f"[DRY] saving extracted metadata to {ProfilingConfig.OUTPUT_PATH} as json files"
                )

            assert extracted_metadata is not None
            create_dir_if_not_exists(ProfilingConfig.OUTPUT_PATH)
            for table_metadata in extracted_metadata.tables:
                write_json(f"{ProfilingConfig.OUTPUT_PATH}/{table_metadata.name}.json", table_metadata.model_dump())
        
    else:
        log("[LOG] Skipped metadata extraction")

    # ---------------------------------
    # Summarizing metadata with an llm, if no metadata was extracted at the previous step it will try to read metadata backup at output_path
    if ProfilingConfig.DO_LLM_SUMMARY:
        if ProfilingConfig.DRY_RUN:
            if extracted_metadata is None:
                print(
                    f"[DRY] run_metadata_llm_summary: summarizing metadata saved @ {ProfilingConfig.OUTPUT_PATH}"
                )
            else:
                print(
                    f"[DRY] run_metadata_llm_summary: summarizing metadata from {len(extracted_metadata.tables)} tables"
                )

        run_metadata_llm_summary(
            db_metadata=extracted_metadata, report_filename="ty_gemini", llm=llm
        )

    else:
        log("[LOG] Skipped llm summary")


def test():
    print(LengthMetaData(min=1, average=2, max=3).model_dump())


if __name__ == "__main__":
    llm = Gemini()
    run(llm)
