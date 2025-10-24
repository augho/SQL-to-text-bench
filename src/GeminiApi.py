from google import genai
from dotenv import load_dotenv
from src.common.utils import str_to_json
from pydantic import BaseModel, Field
from typing import Type
from src.MetadataModel import TableMetadata


class ModelOutput(BaseModel):
    success: bool
    error: str | None
    data: str


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
    ) -> ModelOutput:
        prompt = table_summarization_prompt_init(table_metadata)

        response = self._generate_json(prompt, TableDescription)

        if response is None:
            return ModelOutput(False, "Generation failed", None)

        parse_result = str_to_json(response)

        if parse_result is None:
            return ModelOutput(False, "Model didn't respond with valid json", response)

        return ModelOutput(success=True, error=None, data=parse_result)


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


class FieldDescription(BaseModel):
    """A structured description of an SQL table's field"""

    name: str = Field(description="Name of the field")
    descrition: str = Field(description="Description of the field")


class TableDescription(BaseModel):
    """A structured description of an SQL table and its fields"""

    columns: list[FieldDescription] = Field(
        description="A list of the table's fields decription"
    )
    table: str = Field(description="SQL table description")
