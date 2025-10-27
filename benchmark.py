import argparse
from src.lib.Config import Config

from src.bench.BenchInput import BenchInput
from src.bench.BenchOutput import BenchOutput
from src.bench.Processer import Processer
from src.lib.SqliteConnector import SqliteConnector
from src.lib.utils import (
    check_equality,
    write_json,
    json_to_str,
    human_in_the_loop,
    read_json,
    log,
)
from src.bench.AiInsightApi import AiInsightApi
from src.bench.BenchConfig import (
    BenchConfig,
    RunType,
    arg_appsettings_validate,
    arg_run_type_validate,
)


def construct_input(input_path: str) -> list[BenchInput]:
    json_file = read_json(input_path)
    return list(map(lambda j: BenchInput.init_from_json(j), json_file["input"]))


def test_db():
    db = SqliteConnector(BenchConfig.DB_CONN_STRING, do_logging=BenchConfig.DO_LOGGING)

    assert db.test(), "Connection test failed"

    log("[LOG] CONN TEST SUCCESS")

    assert db.has_table("Album")

    log("[LOG] TABLE CHECK SUCCESS")

    db.do_logging = False
    assert db.select("this is not sql") is None
    db.do_logging = BenchConfig.DO_LOGGING

    # print(db.tables_names())
    query_res_1 = db.select("SELECT AlbumId, ArtistId, Title FROM Album LIMIT 3;")
    query_res_2 = db.select("SELECT Title, AlbumId, ArtistId FROM Album LIMIT 5;")

    assert query_res_1 is not None
    assert query_res_2 is not None

    assert isinstance(query_res_1, list) and isinstance(query_res_2, list)

    assert check_equality(query_res_1, query_res_2)

    log("[LOG] RESULT COMPARISON CHECK SUCCESS")


def test_api():
    agent = AiInsightApi(BenchConfig.API_HOSTNAME, BenchConfig.API_PORT)
    agent.test(
        endpoint="/api/v1/user/1",
        expected={
            "userId": 1,
            "username": "alice",
            "email": "alice@example.com",
            "createdAt": "2025-10-15T12:15:08.499296",
        },
    )

    log("[LOG] API TEST SUCCESS")


def run_bench():

    bench_inputs = construct_input(BenchConfig.DATASET_PATH)
    agent = AiInsightApi(BenchConfig.API_HOSTNAME, BenchConfig.API_PORT)

    # TODO remove
    # Testing with one request for now
    # bench_inputs = [bench_inputs[0]]

    log("\n======= RUN DETAILS =======")
    log(f"# of inputs: {len(bench_inputs)}")
    log(f"use_easy_question: {BenchConfig.USE_EASY_QUESTION}")
    log(f"output_path: {BenchConfig.OUTPUT_PATH}")
    log(f"Max RPM: {BenchConfig.MAX_RPM}")
    log("========            =======\n")

    proceed_confirmation = (
        True
        if BenchConfig.SKIP_INTERACTIONS or BenchConfig.DRY_RUN
        else human_in_the_loop("Do you wish to continue (y/n)?")
    )

    if not proceed_confirmation:
        log("[LOG] Aborting query benchmark run")
        return

    log("\n======== STARTING BENCHMARK ========")
    if BenchConfig.DRY_RUN:
        log("[DRY] Prompting Agent..")
    else:
        bench_outputs = agent.chain_ask(bench_inputs, BenchConfig.USE_EASY_QUESTION)

        output = {
            "output": [
                b.as_dict(with_easy_question=BenchConfig.USE_EASY_QUESTION)
                for b in bench_outputs
            ]
        }

        success = write_json(BenchConfig.OUTPUT_PATH, output)

        if not success:
            log("Here is the ouput though:")
            log(json_to_str(output))


def run_analysis():
    assert BenchConfig.BENCH_REPORT_PATH.find(
        "."
    ) == BenchConfig.BENCH_REPORT_PATH.rfind("."), "Report path must have only one '.'"

    report = read_json(BenchConfig.BENCH_REPORT_PATH)

    bench_outputs: list[BenchOutput] = [
        BenchOutput.from_dict(o) for o in report["output"]
    ]

    processer = Processer(BenchConfig.DB_CONN_STRING, bench_outputs)

    # Changes like so: out/foo.json -> out/foo.stats.json
    stats_filepath = ".stats.".join(BenchConfig.BENCH_REPORT_PATH.split("."))

    if BenchConfig.SAVE_STATS:
        if BenchConfig.DRY_RUN:
            log(f"[DRY] writing statistics to {stats_filepath}")
        else:
            stats = processer.construct_stats()
            write_json(stats_filepath, stats)

    if BenchConfig.DO_ERROR_CHART:
        if BenchConfig.DRY_RUN:
            log("[DRY] creating generation graph")
        else:
            assert stats_filepath.endswith(".stats.json")
            gen_chart_output_path = (
                stats_filepath.removesuffix(".stats.json") + ".success_graph.png"
            )
            Processer.generate_success_graph(stats_filepath, gen_chart_output_path)
            log(f"Generated generation graph at {gen_chart_output_path}")

    if BenchConfig.DO_ERROR_CHART:
        if BenchConfig.DRY_RUN:
            log("[DRY] creating error graph")
        else:
            assert stats_filepath.endswith(".stats.json")

            err_chart_output_path = (
                stats_filepath.removesuffix(".stats.json") + ".err_graph.png"
            )
            Processer.generate_error_graph(stats_filepath, err_chart_output_path)

            log(f"Generated generation graph at {err_chart_output_path}")

    log("[LOG] Analysis performed successfully")


def run(run_type: RunType, dry_run: bool, do_logging: bool, skip_interactions: bool):
    BenchConfig.init(
        BenchConfig.create_from_appsettings(
            "./appsettings.json",
            dry_run=dry_run,
            do_logging=do_logging,
            skip_interactions=skip_interactions,
            run_type=run_type,
        )
    )

    if BenchConfig.RUN_TEST:
        match BenchConfig.RUN_TYPE:
            case RunType.ANALYSIS:
                test_db()
            case RunType.BENCHMARK:
                test_api()
            case RunType.BOTH:
                test_api()
                test_db()

    if BenchConfig.RUN_TYPE in (RunType.BOTH, RunType.BENCHMARK):
        if BenchConfig.DRY_RUN:
            print(
                f"[DRY] run_bench Running benchmark on {BenchConfig.API_HOSTNAME}:{BenchConfig.API_PORT}"
            )
        run_bench()

    if BenchConfig.RUN_TYPE in (RunType.BOTH, RunType.ANALYSIS):
        if BenchConfig.DRY_RUN:
            print(
                f"[DRY] run_analysis on benchmark results in {BenchConfig.BENCH_REPORT_PATH}"
            )
        run_analysis()

    # TODO run a comparison between the BenchOutput and the dataset


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Tool to evaluate the performance of an agent on txt-to-sql tasks"
    )
    parser.add_argument(
        "-s", "--silent", action="store_true", default=False, help="Disable logging"
    )
    parser.add_argument(
        "-d", "--dry-run", action="store_true", default=False, help="Do a dry run"
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        default=False,
        help="Skip HIL interactions",
    )
    parser.add_argument(
        "--appsettings",
        type=arg_appsettings_validate,
        default="./appsettings.json",
        help="Json settings file (default=./appsettings.json)",
    )
    parser.add_argument(
        "-t",
        "--run-type",
        type=arg_run_type_validate,
        default="read",
        help="Specify the output format either analysis, bench or all(unsupported)",
        required=True,
    )

    args = parser.parse_args()

    run(
        run_type=args.run_type,
        do_logging=not args.silent,
        dry_run=args.dry_run,
        skip_interactions=args.yes,
    )
