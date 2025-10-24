from src.SqliteConnector import SqliteConnector
from src.common.utils import (
    log,
    write_json,
    read_dir_files,
    run_rate_limited_tasks,
    human_in_the_loop,
    sqlite_export,
)
from src.GeminiApi import Gemini, ModelOutput, TableDescriptionOutput, TableDescription
from src.common.string_utils import table_desc_creation_str, field_desc_creation_str
from src.CliConfig import CliConfig, OutputFormat
from src.MetadataModel import (
    DatabaseMetadata,
    LengthMetaData,
    TableMetadata,
    ColumnMetadata,
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
                "No metadata found in folder", CliConfig.get().output_path
            )

        return DatabaseMetadata(name=foldername, tables=tables)
    except FileNotFoundError as err:
        print(err)
        return None

def run_metadata_extraction() -> DatabaseMetadata:
    db = SqliteConnector(CliConfig.get().filename)
    db_metadata = {}
    tables: list[TableMetadata] = []
    for tablename in db.list_tables():
        result = table_profile(db=db, tablename=tablename, sample_size=10)
        db_metadata[tablename] = result
        tables.append(result)

    return DatabaseMetadata(name=CliConfig.get().filename, tables=tables)


# TODO fix this fn
def fix_generated_output(
    generation_result: dict[str, TableDescriptionOutput], db_metadata: DatabaseMetadata
) -> None:
    
    for tablename, model_output in generation_result.items():
        truth = db_metadata.find_table_by_name(tablename)
        assert truth is not None

        if not model_output.success:
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
    db_metadata: DatabaseMetadata | None, report_filename: str
) -> None:
    # TODO add a custom folder
    # If no extraction we read the output folder for backed up file
    if not CliConfig.get().do_extraction:
        db_metadata = read_metadata_backup_folder(CliConfig.get().output_path)

    if db_metadata is None:
        log("Aborting llm summary")
        return

    gemini = Gemini()
    generations_output: dict[str, ModelOutput] = {}

    run_args = [(table, generations_output) for table in db_metadata.tables]

    log("\n======= RUN DETAILS =======")
    log(f"llm_model: {gemini.model_name}")
    log(f"# of input tables: {len(run_args)}")
    log(f"table_list: {[x[0].name for x in run_args]}")
    log(f"output_path: {CliConfig.get().output_path}/{report_filename}.llm.json")
    log(f"Max RPM: {CliConfig.get().max_rpm}")
    log("========            =======\n")

    # If no logging set we don't ask for confirmation
    proceed_confirmation = (
        True
        if CliConfig.get().skip_human_in_the_loop
        else human_in_the_loop("Do you wish to continue (y/n)?")
    )

    if not proceed_confirmation:
        log("\n[WARN] Aborting llm table summarization !\n")
        return

    # TODO add a way to read from a saved output instead
    # ---------------------------------
    # Rate limited api calls
    def generation_cb(
        table_metadata: TableMetadata, output_dict: dict[str, ModelOutput]
    ):
        result = gemini.summarize_table_metadata(table_metadata)
        output_dict[table_metadata.name] = result

    run_rate_limited_tasks(
        cb=generation_cb,
        cb_args=run_args,
        max_rpm=CliConfig.get().max_rpm,
        do_logging=CliConfig.get().do_logging,
    )

    
    # Ensures model didn't hallucinate table or field name 
    fix_generated_output(generations_output, db_metadata) 

    # generations_output = json_import("out/profiles/Chinook/ty_gemini.llm.json")

    export_success = export_tool(
        generations_output,
        f"{CliConfig.get().output_path}/{report_filename}.llm",
        CliConfig.get().output_format,
    )

    

    success_count = 0
    error_count = 0
    for model_output in generations_output.values():
        if model_output.success:
            success_count += 1
        else:
            error_count += 1

    log("\n======= RUN RESULT =======")
    log(f"# success: {success_count}")
    log(f"# errors: {error_count}")
    log(
        f"\n[LOG] Summary generated at {CliConfig.get().output_path}/{report_filename}.llm.json\n"
    )

    if not export_success:
        log("[ERR] Couldn't export your data")
        log(str({k: v.model_dump() for k, v in generations_output.items()}))


def export_tool(
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


def run():
    CliConfig.init()

    print(CliConfig.get())
    print()

    extracted_metadata = None
    if CliConfig.get().dry_run:
        log(
            f"[DRY] run_metadata_extraction: reading metadata from {CliConfig.get().filename}"
        )
        if CliConfig.get().save_metadata:
            log(f"[DRY] saving extracted metadata to {CliConfig.get().output_path} as json files")
    elif CliConfig.get().do_extraction:
        extracted_metadata = run_metadata_extraction()
        if CliConfig.get().save_metadata:
            for table_metadata in extracted_metadata.tables:
                write_json(f"{CliConfig.get().output_path}/{table_metadata.name}.json", table_metadata.model_dump())
    else:
        log("[LOG] Skipped metadata extraction")

    if CliConfig.get().dry_run:
        if extracted_metadata is None:
            log(
                f"[DRY] run_metadata_llm_summary: summarizing metadata saved @ {CliConfig.get().output_path}"
            )
        else:
            log(
                f"[DRY] run_metadata_llm_summary: summarizing metadata from {len(extracted_metadata.tables)} tables"
            )

    elif CliConfig.get().do_llm_summary:
        run_metadata_llm_summary(
            db_metadata=extracted_metadata, report_filename="ty_gemini"
        )
    else:
        log("[LOG] Skipped llm summary")


def test():
    print(LengthMetaData(min=1, average=2, max=3).model_dump())


if __name__ == "__main__":
    run()
