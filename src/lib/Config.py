from __future__ import annotations
import argparse
from enum import Enum
from pydantic import BaseModel
import os


class OutputFormat(Enum):
    SQLITE = ("sqlite",)
    JSON = ("json",)
    CSV = "csv"


class Config(BaseModel):
    DO_LOGGING: bool 
    DB_CONN_STRING: str 
    MAX_RPM: int 
    OUTPUT_PATH: str 
    DRY_RUN: bool 
    SKIP_INTERACTIONS: bool

    # Must be defined as static and not class otherwise pyantic raises
    @staticmethod
    def init(config: Config):
        Config.DO_LOGGING = config.DO_LOGGING
        Config.DB_CONN_STRING = config.DB_CONN_STRING
        Config.MAX_RPM = config.MAX_RPM
        Config.OUTPUT_PATH = config.OUTPUT_PATH
        Config.DRY_RUN = config.DRY_RUN
        Config.SKIP_INTERACTIONS = config.SKIP_INTERACTIONS 

    @staticmethod
    def arg_max_rpm_validate(v) -> int:
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

    @staticmethod
    def arg_output_path_validate(v) -> str:
        v = str(v)
        if len(v) == 0:
            raise argparse.ArgumentTypeError("Arg --output-path can't be empty")
        
        v = v[:-1] if v.endswith("/") else v

        if os.path.isfile(v):
            raise argparse.ArgumentTypeError("Arg --output-path must be a directory not a file")
        if not os.path.isdir(v):
            raise argparse.ArgumentTypeError("Arg --output-path must be an existing directory")
        
        return v
