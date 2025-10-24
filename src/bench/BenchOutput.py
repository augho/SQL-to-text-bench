from __future__ import annotations

from src.bench.BenchInput import BenchInput


class BenchOutput:
    def __init__(
        self, matching_input: BenchInput, generated_sql: str | None, error: str | None
    ) -> None:
        self.matching_input: BenchInput = matching_input
        self.generated_sql: str | None = generated_sql
        self.error: str | None = error

    def as_dict(self, with_easy_question: bool = False) -> dict:
        return {
            "list_id": self.matching_input.get_list_id().value,
            "input_id": self.matching_input.id,
            "question": self.matching_input.easy_question
            if with_easy_question
            else self.matching_input.question,
            "correct_sql": self.matching_input.sql,
            "generated_sql": self.generated_sql,
            "error": self.error,
        }

    @staticmethod
    def from_dict(bench_output: dict) -> BenchOutput:
        bench_output.setdefault("question", "")
        return BenchOutput(
            matching_input=BenchInput(
                id=bench_output["input_id"],
                easy_question=None,
                question=bench_output["question"],
                sql=bench_output["correct_sql"],
            ),
            generated_sql=bench_output["generated_sql"],
            error=bench_output["error"],
        )
