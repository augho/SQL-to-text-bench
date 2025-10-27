from __future__ import annotations

from src.lib.Config import Config, OutputFormat
import argparse


class ProfilingConfig(Config):
    OUTPUT_FORMAT: OutputFormat
    DO_EXTRACTION: bool
    DO_LLM_SUMMARY: bool
    SAVE_METADATA: bool

    @classmethod
    def init(cls, config: ProfilingConfig):
        super().init(config)
        cls.OUTPUT_FORMAT = config.OUTPUT_FORMAT
        cls.DO_EXTRACTION = config.DO_EXTRACTION
        cls.DO_LLM_SUMMARY = config.DO_LLM_SUMMARY
        cls.SAVE_METADATA = config.SAVE_METADATA

    @staticmethod
    def create_from_parser() -> ProfilingConfig:
        parser = argparse.ArgumentParser(
            description="Tool to read a database and generate a description of its fields and tables using a LLM"
        )

        # ----------------------------------------------
        # Tweak input
        parser.add_argument("db_conn_string", type=str, help="Path to the sqlite db file")

        # ----------------------------------------------
        # Tweak execution
        parser.add_argument(
            "-s", "--silent", action="store_true", default=False, help="Disable logging"
        )

        parser.add_argument(
            "-m",
            "--max-rpm",
            type=Config.arg_max_rpm_validate,
            default=9,
            help="Maximum # of requests per minute sent to the LLM api (to disable rate limit set to -1)",
        )

        parser.add_argument(
            "-y",
            "--yes",
            action="store_true",
            default=False,
            help="Skip HIL interactions",
        )
        # -----------------------------------------------
        # Tweak output
        parser.add_argument(
            "-o",
            "--output-path",
            type=str,
            default="",
            help="Folder where the output will be written",
            required=True
        )

        parser.add_argument(
            "-f",
            "--output-format",
            type=ProfilingConfig.arg_output_format_validate,
            default="json",
            help="Specify the output format json(default), sqlite, csv(unsupported)",
        )
        parser.add_argument(
            "--save-metadata",
            action="store_true",
            default=False,
            help="Save the extracted metadata into the output folder",
        )

        # -----------------------------------------------
        # Skip some execution
        parser.add_argument(
            "--no-extraction",
            action="store_true",
            default=False,
            help="[DEV] Skip the sql part and reads metadata from json files",
        )
        parser.add_argument(
            "--no-llm",
            action="store_true",
            default=False,
            help="[DEV] Skip the llm querying",
        )

        parser.add_argument("--dry-run", action="store_true", default=False)

        args = parser.parse_args()

        return ProfilingConfig(
            DO_LOGGING=not args.silent,
            DB_CONN_STRING= args.db_conn_string,
            MAX_RPM=args.max_rpm,
            OUTPUT_PATH=args.output_path,
            OUTPUT_FORMAT=args.output_format,
            DO_EXTRACTION=not args.no_extraction,
            DO_LLM_SUMMARY=not args.no_llm,
            DRY_RUN=args.dry_run,
            SAVE_METADATA=args.save_metadata,
            SKIP_INTERACTIONS=args.yes
        )
    
    @staticmethod
    def arg_output_format_validate(v) -> OutputFormat:
        match v:
            case "sqlite":
                return OutputFormat.SQLITE
            case "json":
                return OutputFormat.JSON
            case _:
                raise argparse.ArgumentTypeError(
                    "Supported output types are 'sqlite' and 'json'"
                )