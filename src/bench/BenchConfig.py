from __future__ import annotations
import datetime
import argparse
import os
from enum import Enum
from pydantic import BaseModel, ValidationError

from src.lib.Config import Config
from src.lib.utils import read_json


class BenchConfig(Config):
    # From CLI
    RUN_TYPE: RunType

    # From appsettings.common
    RUN_TEST: bool

    # From appsettings.bench
    DATASET_PATH: str
    API_HOSTNAME: str
    API_PORT: int
    OUTPUT_FILENAME: str
    USE_EASY_QUESTION: bool

    # From appsettings.analysis
    BENCH_REPORT_PATH: str
    SAVE_STATS: bool
    DO_GENERATION_CHART: bool
    DO_ERROR_CHART: bool

    @classmethod
    def init(cls, config: BenchConfig):
        super().init(config=config)

        cls.RUN_TYPE = config.RUN_TYPE
        cls.RUN_TEST = config.RUN_TEST

        cls.DATASET_PATH = config.DATASET_PATH
        cls.API_HOSTNAME = config.API_HOSTNAME
        cls.API_PORT = config.API_PORT
        cls.OUTPUT_FILENAME = config.OUTPUT_FILENAME
        cls.USE_EASY_QUESTION = config.USE_EASY_QUESTION

        cls.BENCH_REPORT_PATH = config.BENCH_REPORT_PATH
        cls.SAVE_STATS = config.SAVE_STATS
        cls.DO_GENERATION_CHART = config.DO_GENERATION_CHART
        cls.DO_ERROR_CHART = config.DO_ERROR_CHART

    @staticmethod
    def create_from_appsettings(
        appsettings_path: str,
        dry_run: bool,
        do_logging: bool,
        skip_interactions: bool,
        run_type: RunType,
    ) -> BenchConfig:
        app_settings_content = read_json(appsettings_path)

        try:
            app_settings = AppSettings.model_validate(app_settings_content)
        except ValidationError as err:
            print("App settings has wrong format")
            raise err

        return AppSettings.to_bench_config(
            app_settings, dry_run, do_logging, skip_interactions, run_type
        )
    

class AppSettings(BaseModel):
    common: CommonSettings
    bench: BenchSettings
    analysis: AnalysisSettings

    @staticmethod
    def to_bench_config(
        appsettings: AppSettings,
        dry_run: bool,
        do_logging: bool,
        skip_interactions: bool,
        run_type: RunType,
    ) -> BenchConfig:
        output_file = appsettings.bench.report_filename_prefix
        output_file += datetime.datetime.now().strftime(
            appsettings.bench.report_filename_prefix
        )
        output_file += ".json"

        # TODO run checks on argument and adapt the appsettings file
        return BenchConfig(
            RUN_TYPE=run_type,
            DO_LOGGING=do_logging,
            DRY_RUN=dry_run,
            SKIP_INTERACTIONS=skip_interactions,
            # common
            RUN_TEST=appsettings.common.run_test,
            # bench
            DATASET_PATH=appsettings.bench.dataset_path,
            API_HOSTNAME=appsettings.bench.api_hostname,
            OUTPUT_PATH=appsettings.bench.output_folder,
            API_PORT=appsettings.bench.api_port,
            MAX_RPM=appsettings.bench.api_max_rpm,
            OUTPUT_FILENAME=output_file,
            USE_EASY_QUESTION=appsettings.bench.use_easy_question,
            # analysis
            BENCH_REPORT_PATH=appsettings.analysis.bench_report_path,
            DB_CONN_STRING=appsettings.analysis.sqlite_db_path,
            SAVE_STATS=appsettings.analysis.save_stats_file,
            DO_GENERATION_CHART=appsettings.analysis.do_generation_chart,
            DO_ERROR_CHART=appsettings.analysis.do_error_chart,
        )


class CommonSettings(BaseModel):
    run_test: bool


class BenchSettings(BaseModel):
    dataset_path: str
    api_hostname: str
    api_port: int
    output_folder: str
    report_filename_prefix: str
    timestamp_format: str
    use_easy_question: bool
    api_max_rpm: int


class AnalysisSettings(BaseModel):
    bench_report_path: str
    sqlite_db_path: str
    save_stats_file: bool
    do_error_chart: bool
    do_generation_chart: bool


class RunType(Enum):
    ANALYSIS = "analysis"
    BENCHMARK = "benchmark"
    BOTH ="both"


def arg_appsettings_validate(v) -> str:
    v = str(v)
    
    if not v.endswith(".json"):
        raise argparse.ArgumentTypeError("Arg --appsettings must be a json file")
        

    if os.path.isdir(v):
        raise argparse.ArgumentTypeError("Arg --appsettings must be a file not a directory")
    if not os.path.isfile(v):
        raise argparse.ArgumentTypeError("Arg --appsettings must be an existing file")
    
    
    return v

def arg_run_type_validate(v) -> RunType:
    v = str(v)
    match v:
        case "analysis":
            return RunType.ANALYSIS
        case "bench":
            return RunType.BENCHMARK
        case _:
            raise argparse.ArgumentTypeError(f"Unsupported value for --run-type '{v}'")