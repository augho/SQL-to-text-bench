from __future__ import annotations
import argparse
from enum import Enum


class OutputFormat(Enum):
    SQLITE = ("sqlite",)
    JSON = ("json",)
    CSV = "csv"


class CliConfig:
    _config: CliConfig | None

    def __init__(self, args: argparse.Namespace) -> None:
        self.filename: str = args.filename
        self.do_logging: bool = not args.silent
        self.max_rpm: int = args.max_rpm
        self.output_path: str = args.output_path
        self.output_format: OutputFormat = args.output_format
        self.do_extraction: bool = not args.no_extraction
        self.do_llm_summary: bool = not args.no_llm
        self.dry_run: bool = args.dry_run
        self.save_metadata: bool = args.save_metadata

    def __str__(self) -> str:
        return f"""CliConfig(
            filename={self.filename},
            do_logging={self.do_logging},
            max_rpm={self.max_rpm},
            output_path={self.output_path},
            output_format={self.output_format},
            do_extraction={self.do_extraction},
            do_llm_summary={self.do_llm_summary},
            dry_run={self.dry_run},
            save_metadata={self.save_metadata})"""

    def __repr__(self) -> str:
        return self.__str__()

    @staticmethod
    def get() -> CliConfig:
        if CliConfig._config is None:
            raise Exception("Init not called")

        return CliConfig._config

    @staticmethod
    def advanced_init(
        filename: str,
        do_logging: bool,
        max_rpm: int,
        output_path: str,
        output_format: OutputFormat,
        do_extraction: bool,
        do_llm_summary: bool,
        dry_run: bool,
        save_metadata: bool,
    ) -> None:
        args = argparse.Namespace(
            filename=filename,
            silent=not do_logging,
            max_rpm=max_rpm,
            output_path=output_path,
            output_format=output_format,
            no_extraction=not do_extraction,
            no_llm=not do_llm_summary,
            dry_run=dry_run,
            save_metadata=save_metadata,
        )
        CliConfig._config = CliConfig(args)

    @staticmethod
    def init() -> None:
        parser = argparse.ArgumentParser(
            description="Tool to read a database and generate a description of its fields and tables using a LLM"
        )

        # ----------------------------------------------
        # Tweak input
        parser.add_argument("filename", type=str, help="Path to the sqlite db file")

        # ----------------------------------------------
        # Tweak execution
        parser.add_argument(
            "-s", "--silent", action="store_true", help="Disable logging"
        )

        parser.add_argument(
            "-m",
            "--max-rpm",
            type=max_rpm_check,
            default=9,
            help="Maximum # of requests per minute sent to the LLM api (to disable rate limit set to -1)",
        )

        # -----------------------------------------------
        # Tweak output
        parser.add_argument(
            "-o",
            "--output-path",
            type=str,
            default="",
            help="Folder where the output will be written",
        )

        parser.add_argument(
            "-f",
            "--output-format",
            type=out_type_check,
            default="json",
            help="Specify the output format json(default), sqlite, csv(unsupported)",
        )
        parser.add_argument(
            "--save-metadata",
            action="store_false",
            help="Save the extracted metadata into the output folder",
        )

        # -----------------------------------------------
        # Skip some execution
        parser.add_argument(
            "--no-extraction",
            action="store_true",
            help="[DEV] Skip the sql part and reads metadata from json files",
        )
        parser.add_argument(
            "--no-llm", action="store_true", help="[DEV] Skip the llm querying"
        )

        parser.add_argument("--dry-run", action="store_false")

        args = parser.parse_args()

        CliConfig._config = CliConfig(args)


def max_rpm_check(v) -> int:
    try:
        v = int(v)
        if v > 0:
            return v
        elif v == -1:
            return -1
        else:
            raise argparse.ArgumentTypeError(
                f"'{v}' must be > 0 or set to -1 to disable rate limiting"
            )
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{v}' is not a valid integer")


def out_type_check(v) -> OutputFormat:
    match v:
        case "sqlite":
            return OutputFormat.SQLITE
        case "json":
            return OutputFormat.JSON
        case _:
            raise argparse.ArgumentTypeError(
                "Supported output types are 'sqlite' and 'json'"
            )
