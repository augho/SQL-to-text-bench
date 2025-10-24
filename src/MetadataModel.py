from __future__ import annotations
from typing import Optional, List
from pydantic import BaseModel
import json
from src.common.utils import log


class DatabaseMetadata(BaseModel):
    name: str
    tables: list[TableMetadata]


class TableMetadata(BaseModel):
    name: str
    columns: list[ColumnMetadata]
    row_count: int

    @staticmethod
    def from_file(filepath: str) -> Optional[TableMetadata]:
        try:
            with open(filepath, "r") as json_file:
                result = json.load(json_file)
                return TableMetadata(**result)
        except Exception as err:
            log(f"[ERROR] While reading|parsing the json file {filepath}")
            log(str(err))
            return None


class ColumnMetadata(BaseModel):
    name: str
    declared_type: str
    allows_null: bool
    is_pk: bool
    null_count: int
    non_null_count: int
    distinct_count: int
    min_value: Optional[int]
    max_value: Optional[int]
    length: LengthMetaData
    samples: List[int | str | None]


class LengthMetaData(BaseModel):
    min: int | None
    average: int | None
    max: int | None


if __name__ == "__main__":
    l = LengthMetaData(1, 2, 3)

    print(l.model_dump_json())
