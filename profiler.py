from src.SqliteConnector import SqliteConnector
from src.common.utils import write_json, create_dir_if_not_exists, read_dir_files, read_json, run_rate_limited_tasks, human_in_the_loop, sqlite_export, json_import
from src.GeminiApi import Gemini
from enum import Enum
from src.common.string_utils import table_desc_creation_str, field_desc_creation_str
import argparse

class OutputFormat(Enum):
    SQLITE = 'sqlite',
    JSON = 'json',
    CSV = 'csv'

def table_profile(
        db: SqliteConnector,
        tablename: str,
        topk: int,
        sample_size: int,
        prefix_len: int
    ):
    info: dict[str, str | int | list] = {}
    info["table_name"] = tablename
    info["row_count"] = db.table_row_count(tablename)

    cols = db.table_columns(tablename)
    info["columns"] = []

    for c in cols:
        column_name = c["column_name"]
        column_info = {
            "name": column_name,
            "declared_type": c["column_type"],
            "allows_null": c["allows_null"],
            "is_pk": c["is_pk"]
        }
        null_count, non_null_count = db.count_nulls_and_nonnulls(tablename, column_name)
        column_info["null_count"] = null_count
        column_info["non_null_count"] = non_null_count
        column_info["distinct_count"] = db.distinct_count(tablename, column_name)

        # TODO do max/min only for integer and not for string
        if non_null_count > 0:
            mn, mx = db.min_max_for_column(tablename, column_name)
            column_info["min_value"] = mn
            column_info["max_value"] = mx

        min_len, avg_len, max_len = db.length_stats_sql(tablename, column_name)
        column_info["length"] = {"min": min_len, "average": avg_len, "max": max_len}

        # if non_null_count > 0:
        #     column_info["top_k"] = []
        # else:
        #     column_info["top_k"] = []

        if non_null_count > 0 and sample_size > 0:
            samples = db.sample_values(tablename, column_name, sample_size=sample_size)
            MAX_PREVIEW_SIZE = 5
            column_info["samples"] = samples[: min(MAX_PREVIEW_SIZE, len(samples) -1)]

        info["columns"].append(column_info)

    return info


def run_metadata_extraction(db: SqliteConnector, output_folder_path: str) -> None:
    for tablename in db.list_tables():
        result = table_profile(
            db=db,
            tablename=tablename,
            topk=5, 
            sample_size=10,
            prefix_len= 5
        )

        write_json(f'{output_folder_path}/{tablename}.json', result)

def run_metadata_llm_summary(metadata_folder_path: str, report_filename: str,  max_rpm: int, do_logging: bool) -> None:
    db_meta_data = {}
    for filename in read_dir_files(metadata_folder_path):
        if filename.endswith(".llm.json") or filename.endswith(".llm.sqlite"):
            continue
        tablename = filename.removesuffix(".json")
        table_meta_data = read_json(metadata_folder_path + "/" + filename)

        db_meta_data[tablename] = table_meta_data


    if len(db_meta_data.keys()) == 0:
        raise FileNotFoundError("No metadata found in folder", metadata_folder_path)
    
    

    def generation_cb(tablename, table_metadata, output_dict):
        result = gemini.summarize_table_metadata(table_metadata)
        output_dict[tablename] = result


    gemini = Gemini()
    generations_output = {}

    run_args = [(tablename, metadata, generations_output) for tablename, metadata in db_meta_data.items()]

    if do_logging:
        print("\n======= RUN DETAILS =======")
        print(f"llm_model: {gemini.model_name}")
        print(f"# of input tables: {len(run_args)}")
        print(f"table_list: {[x[0] for x in run_args]}")
        print(f"output_path: {metadata_folder_path}/{report_filename}.llm.json")
        print(f"Max RPM: {max_rpm}")
        print("========            =======\n")

    # If no logging set we don't ask for confirmation
    proceed_confirmation = True if not do_logging else human_in_the_loop(
            "Do you wish to continue (y/n)?"
    )


    if not proceed_confirmation:
        if do_logging:
            print("\n[WARN] Aborting llm table summarization !\n")
        return
    
    # ---------------------------------
    # Rate limited api calls
    # run_rate_limited_tasks(
    #     cb=generation_cb,
    #     cb_args=run_args,
    #     max_rpm=max_rpm,
    #     do_logging=True
    # )

    generations_output = json_import("out/profiles/Chinook/ty_gemini.llm.json")

    # TODO Ensure data structure even when generation fails (only the desc field should be empty and error and succes adapted)
    success = export_tool(generations_output, f"{metadata_folder_path}/{report_filename}.llm", OutputFormat.SQLITE, do_logging)

    if success:
        print("ok")
        return
    else:
        print("err")
        return
    success_count = 0
    error_count = 0
    for tablename, generation_result in generations_output:
        if not isinstance(generation_result, dict):
            error_count += 1
            continue

        # TODO make sure the column names generated match and correct error if not
        
        if generation_result.get("success", False):
            success_count += 1
        else:
            error_count += 1

    if do_logging:
        print("\n======= RUN RESULT =======")
        print(f"# success: {success_count}")
        print(f"# errors: {error_count}")
        print(f"\n[LOG] Summary generated at {metadata_folder_path}/{report_filename}.llm.json\n")

def export_tool(data: dict, filepath: str, format: OutputFormat, do_logging: bool) -> bool:    
    match format:
        case OutputFormat.JSON:
            return write_json(filepath + ".json", json_output=data)
        case OutputFormat.SQLITE:
            # (tablename, sql query to create table)
            schema: list[tuple[str, str]] = [
                ("table_description", table_desc_creation_str),
                ("field_description", field_desc_creation_str)
            ]
            sql_data: list[list[tuple]] = [[], []]

            # Defining schema
            for tablename, _ in data.items():
                schema.append((tablename, table_desc_creation_str))
            # Defining tuples to be inserted
            field_count: int = 0
            for i, (k, gen_data) in enumerate(data.items()):
                # ---- TABLE DESCRIPTIONS
                # (id, name, desc)
                sql_data[0].append((i, k, None if not gen_data["success"] else gen_data["data"]["table"]))
                # ---- FIELD DESCRIPTIONS
                # (id, table_id, name, desc)
                fields = gen_data["data"]["columns"]

                for f in fields:
                    sql_data[1].append((field_count, i, f["name"], f["descrition"]))
                    field_count += 1


            return sqlite_export(sql_data, schema, filepath + '.sqlite', do_logging)
        case _:
            raise ValueError(f"Output format unsupported: {format}")

def run():
    parser = argparse.ArgumentParser(description="Tool to read a database and generate a description of its fields and tables using a LLM")

    def max_rpm_check(v):
        try:
            v = int(v)
            if v > 0:
                return v
            elif v == -1:
                return -1
            else:
                raise argparse.ArgumentTypeError(f"'{v}' must be > 0 or set to -1 to disable rate limiting")
        except ValueError:
                raise argparse.ArgumentTypeError(f"'{v}' is not a valid integer")

    def out_type_check(v):
        match v:
            case 'sqlite':
                return 'sqlite'
            case 'json':
                return 'json'
            case _:
                raise argparse.ArgumentTypeError("Supported output types are 'sqlite' and 'json'")
    
    # ----------------------------------------------
    # Tweak input
    parser.add_argument("filename", type=str, help="Path to the sqlite db file")

    # ----------------------------------------------
    # Tweak execution
    parser.add_argument("-s", "--silent", action="store_true",
                        help="Disable logging")

    parser.add_argument("-m", "--max-rpm", type=max_rpm_check, default=9, help="Maximum # of requests per minute sent to the LLM api (to disable rate limit set to -1)")

    # -----------------------------------------------
    # Tweak output
    parser.add_argument("-o", "--output-path", type=str, default="",
                        help="Folder where the output will be written")
    
    parser.add_argument("-f", "--output-format", type=out_type_check, default='json', help="Specify the output format json(default), sqlite, csv(unsupported)")
    

    # -----------------------------------------------
    # Skip some execution
    parser.add_argument("--no-extraction", action="store_true", help="[DEV] Skip the sql part and reads metadata from json files")
    parser.add_argument("--no-llm", action="store_true", help="[DEV] Skip the llm querying")

    parser.add_argument("--dry-run", action="store_false")

    args = parser.parse_args()
    

    # 4. Use the arguments
    print("--- Starting Script ---")
    print(f"File to process: {args.filename}")
    print(f"Silent mode: {args.silent}")
    print(f"Out path and format: {args.output_path}, {args.output_format}")
    print(f"RPM: {args.max_rpm}")
    run_metadata_llm_summary(
        metadata_folder_path=args.output_path,
        report_filename="ty_gemini",
        do_logging=not args.silent,
        max_rpm=args.max_rpm
    )
    print("--- Script Finished ---")

    
if __name__ == "__main__":
    db_filepath = "./db/Chinook.db"
    db = SqliteConnector(db_filepath)

    DB_NAME = 'Chinook'
    OUTPUT_FOLDER = f"out/profiles/{DB_NAME}"
    REPORT_NAME = "ty_gemini"
    MAX_RPM = 9
    
    run()

    # DO_METADATA_EXTRACTION = human_in_the_loop(
    #     "Do you want to do run sql queries to extract metadata (y/N) ?",
    #     do_default=(True, 'n')
    # )
    # DO_LLM_SUMMARY = human_in_the_loop(
    #     "Do you want to generate table description w/ a LLM (Y/n) ?",
    #     do_default=(True, 'y')
    # )




    # if DO_METADATA_EXTRACTION:
    #     create_dir_if_not_exists(OUTPUT_FOLDER)
    #     run_metadata_extraction(db, output_folder_path=OUTPUT_FOLDER)
    # if DO_LLM_SUMMARY:
    #     run_metadata_llm_summary(metadata_folder_path=OUTPUT_FOLDER, report_filename=REPORT_NAME, max_rpm=MAX_RPM)





    
    
    