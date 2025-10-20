from src.BenchOutput import BenchOutput
from src.Processer import Processer
from common.utils import read_json, write_json

def run_analysis(filepath: str, db_conn_str: str, do_stats: bool = False, do_err_chart: bool = False, do_gen_chart: bool = False):
    assert filepath.find('.') == filepath.rfind('.'), "Report path must have only one '.'"

    report = read_json(filepath)

    bench_outputs: list[BenchOutput] = [BenchOutput.from_dict(o) for o in report["output"]]

    processer = Processer(db_conn_str, bench_outputs)

    # Changes like so: out/foo.json -> out/foo.stats.json
    stats_filepath = ".stats.".join(filepath.split('.'))

    if do_stats:
        stats = processer.construct_stats()
        write_json(stats_filepath, stats)

    if do_gen_chart:
        Processer.generate_success_graph(stats_filepath)
    if do_err_chart:
        Processer.generate_error_graph(stats_filepath)

    print("[LOG] Analysis performed successfully")

if __name__ == "__main__":
    report_path = "out/report_20251013_111554.json"
    db_conn_str = "./db/Chinook.db"
    run_analysis(report_path, db_conn_str)


