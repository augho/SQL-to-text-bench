from src.SqliteConnector import SqliteConnector
from src.common.utils import write_json, create_dir_if_not_exists, read_dir_files, read_json, run_rate_limited_tasks, human_in_the_loop
from src.GeminiApi import Gemini


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

def run_metadata_llm_summary(metadata_folder_path: str, report_filename: str,  max_rpm: int) -> None:
    db_meta_data = {}
    for filename in read_dir_files(metadata_folder_path):
        if filename.endswith(".llm.json"):
            continue
        tablename = filename.removesuffix(".json")
        table_meta_data = read_json(metadata_folder_path + "/" + filename)

        db_meta_data[tablename] = table_meta_data


    if len(db_meta_data.keys()) == 0:
        print("No metadata found in folder", metadata_folder_path)
        return
    
    

    def generation_cb(tablename, table_metadata, output_dict):
        result = gemini.summarize_table_metadata(table_metadata)
        output_dict[tablename] = result


    gemini = Gemini()
    generations_output = {}

    run_args = [(tablename, metadata, generations_output) for tablename, metadata in db_meta_data.items()]

    print("\n======= RUN DETAILS =======")
    print(f"llm_model: {gemini.model_name}")
    print(f"# of input tables: {len(run_args)}")
    print(f"table_list: {[x[0] for x in run_args]}")
    print(f"output_path: {metadata_folder_path}/{report_filename}.llm.json")
    print(f"Max RPM: {max_rpm}")
    print("========            =======\n")
    proceed_confirmation = human_in_the_loop(
        "Do you wish to continue (y/n)?"
    )

    if not proceed_confirmation:
        print("\n[WARN] Aborting llm table summarization !\n")
        return
    
    run_rate_limited_tasks(
        cb=generation_cb,
        cb_args=run_args,
        max_rpm=max_rpm,
        do_logging=True
    )

    write_json(f"{metadata_folder_path}/{report_filename}.llm.json", json_output=generations_output)

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


    print("\n======= RUN RESULT =======")
    print(f"# success: {success_count}")
    print(f"# errors: {error_count}")
    print(f"\n[LOG] Summary generated at {metadata_folder_path}/{report_filename}.llm.json\n")

    
if __name__ == "__main__":
    db_filepath = "./db/Chinook.db"
    db = SqliteConnector(db_filepath)

    DB_NAME = 'Chinook'
    OUTPUT_FOLDER = f"out/profiles/{DB_NAME}"
    REPORT_NAME = "ty_gemini"
    MAX_RPM = 9
    

    DO_METADATA_EXTRACTION = human_in_the_loop(
        "Do you want to do run sql queries to extract metadata (y/N) ?",
        do_default=(True, 'n')
    )
    DO_LLM_SUMMARY = human_in_the_loop(
        "Do you want to generate table description w/ a LLM (Y/n) ?",
        do_default=(True, 'y')
    )




    if DO_METADATA_EXTRACTION:
        create_dir_if_not_exists(OUTPUT_FOLDER)
        run_metadata_extraction(db, output_folder_path=OUTPUT_FOLDER)
    if DO_LLM_SUMMARY:
        run_metadata_llm_summary(metadata_folder_path=OUTPUT_FOLDER, report_filename=REPORT_NAME, max_rpm=MAX_RPM)





    
    
    