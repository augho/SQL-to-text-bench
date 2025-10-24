from __future__ import annotations
from google import genai
from dotenv import load_dotenv
from pydantic import BaseModel, Field, ValidationError
from typing import Type, TypeVar, Generic
from src.MetadataModel import TableMetadata, ColumnMetadata


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
        return TableDescription(table="", columns=[FieldDescription.from_metadata(f) for f in table_metadata.columns])
class ModelOutput(BaseModel, Generic[T]):
    success: bool
    error: str | None
    data: T


TableDescriptionOutput = ModelOutput[TableDescription]


class Gemini:
    def __init__(self, model_name: str = "gemini-2.5-flash") -> None:
        load_dotenv()
        self.client: genai.Client = genai.Client()
        self.model_name: str = model_name

    def _generate_text(self, prompt: str) -> str | None:
        response = self.client.models.generate_content(
            model=self.model_name, contents=prompt
        )
        return response.text

    def _generate_json(self, prompt: str, response_schema: Type) -> str | None:
        response = self.client.models.generate_content(
            model=self.model_name,
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "response_schema": response_schema,
            },
        )
        return response.text

    def _summarize_db_table(self, tablename: str, db_metadata: dict[str, dict]) -> str:
        if tablename not in db_metadata.keys():
            raise Exception("table not in the db_metadata", tablename)

        return ""

    def summarize_table_metadata(
        self, table_metadata: TableMetadata | str
    ) -> TableDescriptionOutput:
        prompt = table_summarization_prompt_init(table_metadata)

        response = self._generate_json(prompt, TableDescription)

        if response is None:
            return TableDescriptionOutput(success=False, error="Generation failed", data=TableDescription.empty())

        try:
            parse_result = TableDescription.model_validate_json(response)
        except ValidationError:
            parse_result = None


        # parse_result = str_to_json(response)

        if parse_result is None:
            return TableDescriptionOutput(
                success=False,
                error=f"Model didn't respond with valid json: {response}",
                data=TableDescription.empty(),
            )

        return TableDescriptionOutput(success=True, error=None, data=parse_result)


def table_summarization_prompt_init(table_metadata: str | TableMetadata):
    return f"""
Given the following sql table meta data. Give me a short description of each column of the table and then a short descrition of the table.
You MUST respect the given output format which should be ONLY valid json
**Table meta data**
{table_metadata if isinstance(table_metadata, str) else table_metadata.model_dump_json()}

**Output format**
{{
            "columns": [
        {{"<column_name>": "<description>"}},
        {{"<column_name>": "<description>"}},
        ...
    ],
    "table": "<description>"
}}

**Example ouput**

{{
    "columns": [
        {{
            "EmployeeID": "An **INTEGER** column that serves as the **primary key** and a foreign key to the main Employee table (not shown). Its function is to uniquely identify the employee associated with each sales record, establishing the 'who' in the transaction data."
        }},
        {{
            "Quarterly_Revenue": "A **DECIMAL** column that records the financial value, or **sales amount**, generated during a specific quarter. Its purpose is to track the performance metric, representing the 'what' and 'how much' of the employee's sales contribution."
        }}
    ],
    "table": "The **Employee_Sales** table functions as a **fact table** used for sales analysis. Its structure links a specific employee (via **EmployeeID**) to a quantitative performance metric (**Quarterly_Revenue**). Since EmployeeID is not unique across all rows, the table is designed to capture **time-series or periodic metrics** (like quarterly data) for each employee, making it ideal for tracking performance trends and aggregations."
}}"""
