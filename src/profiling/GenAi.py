from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Type
from pydantic import ValidationError

from src.profiling.Models import TableDescription, TableMetadata, TableDescriptionOutput
from src.lib.Errors import AiApiError

class GenAiApi(ABC):
    @abstractmethod
    def get_model_name(self) -> str:
        pass
    @abstractmethod
    def _generate_text(self, prompt: str) -> str | None:
        pass

    @abstractmethod
    def retry_strategy(self, status_code: int, count: int) -> bool:
        pass

    @abstractmethod
    def _generate_json(self, prompt: str, response_schema: Type) -> str | AiApiError:
        pass

    def summarize_table_metadata(
        self, table_metadata: TableMetadata | str
    ) -> tuple[TableDescriptionOutput, AiApiError | None]:
        prompt = table_summarization_prompt_init(table_metadata)

        response = self._generate_json(prompt, TableDescription)

        if isinstance(response, AiApiError):
            return TableDescriptionOutput(
                success=False,
                error="Api error",
                data=TableDescription.empty()
            ), response

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
            ), None

        return TableDescriptionOutput(success=True, error=None, data=parse_result), None
    


def table_summarization_prompt_init(table_metadata: str | TableMetadata):
    return f"""
Given the following sql table meta data. Give me a short description of each column of the table and then a short descrition of the table.
You MUST respect the given output format which should be ONLY valid json
**Table meta data**
{table_metadata if isinstance(table_metadata, str) else table_metadata.model_dump_json()}

**Output format**
{{
    "columns": [
        {{
            "name": "<column_name>",
            "description": "<description>"
        }},
        {{
            "name":"<column_name>",
            "description": "<description>"
        }},
        ...
    ],
    "table": "<description>"
}}

**Example ouput**

{{
    "columns": [
        {{
            "name": "EmployeeID",
            "description: "An **INTEGER** column that serves as the **primary key** and a foreign key to the main Employee table (not shown). Its function is to uniquely identify the employee associated with each sales record, establishing the 'who' in the transaction data."
        }},
        {{
            "name": "Quarterly_Revenue",
            "description": "A **DECIMAL** column that records the financial value, or **sales amount**, generated during a specific quarter. Its purpose is to track the performance metric, representing the 'what' and 'how much' of the employee's sales contribution."
        }}
    ],
    "table": "The **Employee_Sales** table functions as a **fact table** used for sales analysis. Its structure links a specific employee (via **EmployeeID**) to a quantitative performance metric (**Quarterly_Revenue**). Since EmployeeID is not unique across all rows, the table is designed to capture **time-series or periodic metrics** (like quarterly data) for each employee, making it ideal for tracking performance trends and aggregations."
}}"""


table_desc_creation_str = """
CREATE TABLE IF NOT EXISTS table_description (
    id INT PRIMARY KEY,
    name TEXT,
    description TEXT
)
"""

field_desc_creation_str = """
CREATE TABLE IF NOT EXISTS field_description (
    id INT PRIMARY KEY,
    table_id INT,
    name TEXT,
    description TEXT
)
"""