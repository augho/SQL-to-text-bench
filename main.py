import datetime

from src.BenchInput import BenchInput
from src.SqliteConnector import SqliteConnector
from common.utils import check_equality, write_json, json_to_str, human_in_the_loop, read_json
from src.AiInsightApi import AiInsightApi



def construct_input(input_path: str) -> list[BenchInput]:
    json_file = read_json(input_path)
    return list(
        map(lambda j: BenchInput.init_from_json(j), json_file["input"])
    )


def test_db(db_filepath, do_logging: bool = True):
    db = SqliteConnector(db_filepath, do_logging=do_logging)
    assert db.test(), "Connection test failed"

    if do_logging:
        print("[LOG] CONN TEST SUCCESS")
        
    assert db.has_table('Album')
    if do_logging:
        print("[LOG] TABLE CHECK SUCCESS")

    assert db.execute_query("this is not sql") is None

    # print(db.tables_names())
    query_res_1 = db.execute_query("SELECT AlbumId, ArtistId, Title FROM Album LIMIT 3;")
    query_res_2 = db.execute_query("SELECT Title, AlbumId, ArtistId FROM Album LIMIT 5;")

    assert query_res_1 is not None
    assert query_res_2 is not None

    assert isinstance(query_res_1, list) and isinstance(query_res_2, list)
    
    assert check_equality(query_res_1, query_res_2)
    if do_logging:
        print("[LOG] RESULT COMPARISON CHECK SUCCESS")


def test_api(hostname: str, port: int, do_logging: bool = True):
    agent = AiInsightApi(hostname, port)
    agent.test(
        endpoint="/api/v1/message/1",
        expected={
            "messageId": 1,
            "conversationId": 1,
            "senderType": 0,
            "messageText": "Hello, this is Alice!",
            "requiresApproval": False,
            "linkedMessageId": None,
            "approvalStatus": 0,
            "approvedAt": None
            }
    )
    if do_logging:
        print("[LOG] API TEST SUCCESS")


def run_bench(
    api_hostname: str,
    api_port: int,
    input_path: str,
    output_path: str,
    use_easy_question: bool = False,
    do_logging: bool = True
    ):

    bench_inputs = construct_input(input_path)
    agent = AiInsightApi(api_hostname, api_port)
    
    # TODO remove
    # Testing with one request for now
    # bench_inputs = [bench_inputs[0]]

    print("\n======= RUN DETAILS =======")
    print(f"# of inputs: {len(bench_inputs)}")
    print(f"use_easy_question: {use_easy_question}")
    print(f"output_path: {output_path}")
    print(f"Max RPM: {AiInsightApi.MAX_RPM}")
    print("========            =======\n")
    proceed_confirmation = human_in_the_loop(
        "Do you wish to continue (y/n)?"
    )
    if not proceed_confirmation:
        if do_logging:
            print("[LOG] Aborting query benchmark run")
        return

    if do_logging:
        print("\n======== STARTING BENCHMARK ========")
    bench_outputs = agent.chain_ask(bench_inputs, use_easy_question, do_logging)

    output = {"output": [b.as_dict(with_easy_question=use_easy_question) for b in bench_outputs]}

    success = write_json(output_path, output)

    if not success:
        print("Here is the ouput though:")
        print(json_to_str(output))


if __name__ == "__main__":
    config = read_json("./appsettings.json")
    
    INPUT_FILEPATH = config["input"]["queriesFilepath"]
    DB_CONN_STR = config["input"]["dbFilepath"]
    API_HOSTNAME = config["input"]["apiHostname"]
    API_PORT = config["input"]["apiPort"]

    # folder + filename prefix + .json
    OUTPUT_FILEPATH = config["output"]["folder"] 
    OUTPUT_FILEPATH += config["output"]["filenamePrefix"] 
    OUTPUT_FILEPATH += datetime.datetime.now().strftime(config["output"]["timestampFormat"])
    OUTPUT_FILEPATH += ".json"

    DO_TEST = config["run"]["doTest"]
    DO_LOGGING = config["run"]["doLogging"]
    USE_EASY_QUESTION = config["run"]["useEasyQuestion"]
    AiInsightApi.MAX_RPM = config["run"]["apiMaxRpm"]

    if DO_TEST:
        test_db(DB_CONN_STR, do_logging=DO_LOGGING)
        test_api(API_HOSTNAME, API_PORT, do_logging=DO_LOGGING)
    
    run_bench(
        api_hostname=API_HOSTNAME,
        api_port=API_PORT,
        input_path=INPUT_FILEPATH,
        output_path=OUTPUT_FILEPATH,
        use_easy_question=USE_EASY_QUESTION,
        do_logging=DO_LOGGING
    )
    # TODO run a comparison between the BenchOutput and the dataset