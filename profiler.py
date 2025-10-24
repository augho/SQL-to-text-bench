from src.SqliteConnector import SqliteConnector
from src.common.utils import (
    log,
    write_json,
    read_dir_files,
    read_json,
    run_rate_limited_tasks,
    human_in_the_loop,
    sqlite_export,
)
from src.GeminiApi import Gemini, ModelOutput
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
        mn, mx = (
            None,
            None
            if non_null_count > 0
            else db.min_max_for_column(tablename, column_name),
        )

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
            length=LengthMetaData(min_len, avg_len, max_len),
            samples=samples,
        )
        columns_metadata.append(col_metadata)

    return TableMetadata(tablename, columns_metadata, row_count)


def read_metadata_backup_folder(foldername: str) -> DatabaseMetadata:
    tables: list[TableMetadata] = []
    for filename in read_dir_files(foldername):
        if not filename.endswith(".json") or filename.endswith(".llm.json"):
            continue

        table = TableMetadata.from_file(foldername + "/" + filename)
        if table is None:
            raise Exception(f"Failed to load table metadata for {filename}")
        tables.append(table)

    if len(tables) == 0:
        raise FileNotFoundError(
            "No metadata found in folder", CliConfig.get().output_path
        )

    return DatabaseMetadata(foldername, tables)


def run_metadata_extraction() -> DatabaseMetadata:
    db = SqliteConnector(CliConfig.get().filename)
    db_metadata = {}
    tables: list[TableMetadata] = []
    for tablename in db.list_tables():
        result = table_profile(db=db, tablename=tablename, sample_size=10)
        db_metadata[tablename] = result
        tables.append(result)

    return DatabaseMetadata(CliConfig.get().filename, tables)


# TODO fix this fn
def fix_generated_output(
    generation_result: dict, db_metadata: DatabaseMetadata
) -> None:
    for tablename, metadat in db_metadata.items():
        if tablename not in generation_result:
            generation_result[tablename] = {
                "description": "",
                "error": "Generation failed",
                "success": False,
            }


def run_metadata_llm_summary(
    db_metadata: DatabaseMetadata | None, report_filename: str
) -> None:
    # TODO add a custom folder
    # If no extraction we read the output folder for backed up file
    if CliConfig.get().do_extraction:
        db_metadata = read_metadata_backup_folder(CliConfig.get().output_path)

    assert db_metadata is not None

    def generation_cb(
        table_metadata: TableMetadata, output_dict: dict[str, ModelOutput]
    ):
        result = gemini.summarize_table_metadata(table_metadata)
        output_dict[table_metadata.name] = result

    gemini = Gemini()
    generations_output: dict[str, ModelOutput] = {}

    run_args = [(table, generations_output) for table in db_metadata.tables]

    log("\n======= RUN DETAILS =======")
    log(f"llm_model: {gemini.model_name}")
    log(f"# of input tables: {len(run_args)}")
    log(f"table_list: {[x[0] for x in run_args]}")
    log(f"output_path: {CliConfig.get().output_path}/{report_filename}.llm.json")
    log(f"Max RPM: {CliConfig.get().max_rpm}")
    log("========            =======\n")

    # If no logging set we don't ask for confirmation
    proceed_confirmation = (
        True
        if not CliConfig.get().do_logging
        else human_in_the_loop("Do you wish to continue (y/n)?")
    )

    if not proceed_confirmation:
        log("\n[WARN] Aborting llm table summarization !\n")
        return

    # TODO add a way to read from a saved output instead
    # ---------------------------------
    # Rate limited api calls
    run_rate_limited_tasks(
        cb=generation_cb,
        cb_args=run_args,
        max_rpm=CliConfig.get().max_rpm,
        do_logging=CliConfig.get().do_logging,
    )

    # Ensures model didn't hallucinate table or field name
    fix_generated_output(generation_result, db_metadata)  # noqa: F821

    # generations_output = json_import("out/profiles/Chinook/ty_gemini.llm.json")

    success = export_tool(
        generations_output,
        f"{CliConfig.get().output_path}/{report_filename}.llm",
        CliConfig.get().output_format,
    )

    # TODO handle failed to export. Log ? or something
    if success:
        print("ok")

    else:
        print("err")

    success_count = 0
    error_count = 0
    for _, model_output in generations_output:
        if not model_output.success:
            success_count += 1
        else:
            error_count += 1

    log("\n======= RUN RESULT =======")
    log(f"# success: {success_count}")
    log(f"# errors: {error_count}")
    log(
        f"\n[LOG] Summary generated at {CliConfig.get().output_path}/{report_filename}.llm.json\n"
    )


def export_tool(
    model_outputs: dict[str, ModelOutput], filepath: str, format: OutputFormat
) -> bool:
    match format:
        case OutputFormat.JSON:
            return write_json(
                filepath + ".json", json_output={k: v for k, v in model_outputs}
            )
        case OutputFormat.SQLITE:
            # (tablename, sql query to create table)
            schema: list[tuple[str, str]] = [
                ("table_description", table_desc_creation_str),
                ("field_description", field_desc_creation_str),
            ]
            sql_data: list[list[tuple]] = [[], []]

            # Defining schema
            for tablename, _ in model_outputs.items():
                schema.append((tablename, table_desc_creation_str))
            # Defining tuples to be inserted
            field_count: int = 0
            for i, (k, gen_data) in enumerate(model_outputs.items()):
                # ---- TABLE DESCRIPTIONS
                # (id, name, desc)
                sql_data[0].append(
                    (
                        i,
                        k,
                        None if not gen_data["success"] else gen_data["data"]["table"],
                    )
                )
                # ---- FIELD DESCRIPTIONS
                # (id, table_id, name, desc)
                fields = gen_data["data"]["columns"]

                for f in fields:
                    sql_data[1].append((field_count, i, f["name"], f["descrition"]))
                    field_count += 1

            return sqlite_export(sql_data, schema, filepath + ".sqlite")
        case _:
            raise ValueError(f"Output format unsupported: {format}")


def run():
    CliConfig.init()

    # 4. Use the arguments
    print("--- Starting Script ---")
    print(CliConfig.get())
    print()

    extracted_metadata = None
    if CliConfig.get().dry_run:
        log(
            f"[DRY] run_metadata_extraction: reading metadata from {CliConfig.get().filename}"
        )
    elif CliConfig.get().do_extraction:
        extracted_metadata = run_metadata_extraction()
        if CliConfig.get().save_metadata:
            for tablename, metadata in extracted_metadata.items():
                write_json(f"{CliConfig.get().output_path}/{tablename}.json", metadata)
    else:
        log("[LOG] Skipped metadata extraction")

    if CliConfig.get().dry_run:
        if extracted_metadata is None:
            log(
                f"[DRY] run_metadata_llm_summary: summarizing metadata saved @ {CliConfig.get().output_path}"
            )
        else:
            log(
                f"[DRY] run_metadata_llm_summary: summarizing metadata from {len(extracted_metadata.keys())} tables"
            )

    elif CliConfig.get().do_llm_summary:
        run_metadata_llm_summary(
            db_metadata=extracted_metadata, report_filename="ty_gemini"
        )
    else:
        log("[LOG] Skipped llm summary")


def test():
    print(LengthMetaData(min=1, average=2, max=3).model_dump_json())


if __name__ == "__main__":
    # run()
    test()
