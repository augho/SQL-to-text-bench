from __future__ import annotations
import json
from typing import Any, TypeVar, Generic

from pydantic import BaseModel, Field

from src.lib.utils import log

# ------------------------------------------------------
# Metadata models


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


# ------------------------------------------------------
# GenAiApi models

T = TypeVar("T")


class FieldDescription(BaseModel):
    """A structured description of an SQL table's field"""

    name: str = Field(description="Name of the field")
    description: str = Field(description="Description of the field")

    @staticmethod
    def from_metadata(field_metadata: ColumnMetadata) -> FieldDescription:
        return FieldDescription(name=field_metadata.name, description="")


class TableDescription(BaseModel):
    """A structured description of an SQL table and its fields"""

    columns: list[FieldDescription] = Field(
        description="A list of the table's fields decription"
    )
    table: str = Field(description="SQL table description")

    @staticmethod
    def empty() -> TableDescription:
        return TableDescription(columns=[], table="")

    @staticmethod
    def from_metadata(table_metadata: TableMetadata) -> TableDescription:
        return TableDescription(
            table="",
            columns=[FieldDescription.from_metadata(f) for f in table_metadata.columns],
        )


class ModelOutput(BaseModel, Generic[T]):
    success: bool
    error: str | None
    data: T


TableDescriptionOutput = ModelOutput[TableDescription]
