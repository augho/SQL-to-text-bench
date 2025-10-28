from __future__ import annotations
from typing import Type
from google import genai
from google.genai.errors import APIError
from dotenv import load_dotenv

from src.profiling.GenAi import GenAiApi
from src.lib.Errors import AiApiError
from src.lib.utils import log, chaos_monkey

"""
.env required setup:

GEMINI_API_KEY=<api_key>

"""

class Gemini(GenAiApi):
    def __init__(self, model_name: str = "gemini-2.5-flash") -> None:
        load_dotenv()
        self.client: genai.Client = genai.Client()
        self.model_name: str = model_name

    def _generate_text(self, prompt: str) -> str | None:
        response = self.client.models.generate_content(
            model=self.model_name, contents=prompt
        )
        return response.text

    def _generate_json(self, prompt: str, response_schema: Type) -> str | AiApiError:
        if chaos_monkey(0.1):
            return AiApiError(code=418, message=repr("Crash test"), details="Controlled failure")
        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config={
                    "response_mime_type": "application/json",
                    "response_schema": response_schema,
                },
            )
            return str(response.text)
        except APIError as e:
            return AiApiError(code=e.code, message=repr(e.message), details=repr(e.details))
            

    def _summarize_db_table(self, tablename: str, db_metadata: dict[str, dict]) -> str:
        if tablename not in db_metadata.keys():
            raise Exception("table not in the db_metadata", tablename)

        return ""
    
    def get_model_name(self) -> str:
        return self.model_name
    
    def retry_strategy(self, status_code: int, count: int) -> bool:
        if count < 1:
            log(f"[WARN] error count incorrect {count} code={status_code}")
            return False
        
        match status_code:
            case 400:
                return False
            
            case 418:
                return True
            
            case _:
                log(f"[WARN] No strategy for gemini error code {status_code}")
                return False
        