from __future__ import annotations
from pydantic import BaseModel
import json
from src.common.utils import log
from typing import Any


class DatabaseMetadata(BaseModel):
    name: str
    tables: list[TableMetadata]

    def find_table_by_name(self, tablename: str) -> TableMetadata | None:
        for table in self.tables:
            if table.name == tablename:
                return table
        return None


class TableMetadata(BaseModel):
    name: str
    columns: list[ColumnMetadata]
    row_count: int

    def find_column_by_name(self, name: str) -> ColumnMetadata | None:
        for col in self.columns:
            if col.name == name:
                return col
        return None

    @staticmethod
    def from_file(filepath: str) -> TableMetadata | None:
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
    min_value: int | None
    max_value: int | None
    length: LengthMetaData
    samples: list[Any]


class LengthMetaData(BaseModel):
    min: int | None
    average: float | None
    max: int | None


if __name__ == "__main__":
    pass
