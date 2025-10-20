import re

from src.BenchOutput import BenchOutput
from src.BenchInput import BenchInput
from src.SqliteConnector import SqliteConnector
from utils.utils import remove_limit_clause, check_equality, read_json, create_graph


class Processer:
    def __init__(self, db_conn_str: str, bench_outputs: list[BenchOutput]) -> None:
        self.outputs: list[BenchOutput] = bench_outputs
        self.db: SqliteConnector = SqliteConnector(db_conn_str)
    
    def inputs(self) -> list[BenchInput]:
        return [o.matching_input for o in self.outputs]
    
    def get_error_stats(self):
        error_count = 0
        error_justifications = []
        for o in self.outputs:
            if o.error is not None:
                error_count += 1
                error_justifications.append(o.error)
        
        return {
            "error_count": error_count,
            "error_justifications": error_justifications
        }
    
    def get_success_rate(self):
        total = len(self.outputs)
        exact_match  = 0
        no_match = 0
        no_match_details = []
        sql_error = 0
        sql_error_details = []

        for o in self.outputs:
            if o.error is not None:
                continue
            
            generated_sql = remove_limit_clause(o.generated_sql or "")

            exact_result_set = self.db.execute_query(o.matching_input.sql)
            llm_result_set = self.db.execute_query(generated_sql)

            # Handling sql errors
            if exact_result_set is None:
                raise Exception("Error with dataset sql", o.matching_input.sql)
            
            if llm_result_set is None:
                sql_error += 1
                sql_error_details.append({
                    "id": o.matching_input.id,
                    "question": o.matching_input.question,
                    "expected": o.matching_input.sql,
                    "generated": o.generated_sql
                })

            else:
                # Comparing set
                set_match = check_equality(llm_result_set, exact_result_set)

                if set_match:
                    exact_match += 1
                else:
                    no_match += 1
                    row_count = len(exact_result_set)
                    llm_row_count = len(llm_result_set)
                    field_count = 0 if row_count == 0 else len(exact_result_set[0])
                    llm_field_count = 0 if llm_row_count == 0 else len(llm_result_set[0])

                    no_match_details.append({
                    "id": o.matching_input.id,
                    "question": o.matching_input.question,
                    "expected": o.matching_input.sql,
                    "generated": o.generated_sql,
                    "result_stats": f"(expected, generated): # of row=({row_count}, {llm_row_count}), # of fields=({field_count}, {llm_field_count})"
                })
            
        return {
            "total": total,
            "exact_match": exact_match,
            "no_match": {
                "count": no_match,
                "details": no_match_details
            },
            "sql_error": {
                "count": sql_error,
                "details": sql_error_details
            }
        }
    
    def construct_stats(self):
        return {
            "success_rate": self.get_success_rate(),
            "error_state": self.get_error_stats(),
        }
    
    @staticmethod
    def generate_error_graph(stats_filepath: str):
        # Bar chart with errors
        # x: errors codes like [ERR1, ERR2] and so on
        # y: count for each category
        assert stats_filepath.endswith(".stats.json")
        stats = read_json(stats_filepath)
        
        errors_justifications: list[str] = stats["error_state"]["error_justifications"]

        err_chart_cat = []
        err_chart_val = []
        err_chart_data: dict[str, int] = {}

        for e in errors_justifications:
            formatted_error = e.split(' ')[0].replace('[', '').replace(']', '')

            if formatted_error not in err_chart_data:
                err_chart_data[formatted_error] = 1
            else:
                err_chart_data[formatted_error] += 1
        
        for k, v in err_chart_data.items():
            err_chart_cat.append(k)
            err_chart_val.append(v)

        create_graph(
            output_path=stats_filepath.removesuffix(".stats.json") + ".err_graph.png",
            categories=err_chart_cat,
            values=err_chart_val,
            xlabel="Error types",
            ylabel="Occurences",
            title="Error chart"
        )

    
    @staticmethod
    def generate_success_graph(stats_filepath: str):
        # Bar chart with the results
        # x: [exact_match, no_match:row_eq, no_match: field_eq, no_match:other, sql_error, error]
        # y: count for each category
        assert stats_filepath.endswith(".stats.json")
        stats = read_json(stats_filepath)
        stats = stats["success_rate"]

        total_count = stats["total"]        
        exact_match_count = stats["exact_match"]
        no_match_count = stats["no_match"]["count"]
        no_match_details = stats["no_match"]["details"]
        sql_error_count = stats["sql_error"]["count"]
        # sql_error_details = stats["sql_error"]["details"]


        def parse_details(no_match_detail: dict) -> str:
            tuple_pattern = r'\(([^()]+)\)' 
            extracted_tuple_str = re.findall(tuple_pattern, no_match_detail["result_stats"])

            expected_row_count, llm_row_count = map(lambda x: int(x), extracted_tuple_str[1].split(','))
            expected_field_count, llm_field_count = map(lambda x: int(x), extracted_tuple_str[2].split(','))

            if expected_row_count == llm_row_count and expected_field_count == llm_field_count:
                return "no_match:other"
            elif expected_row_count == llm_row_count:
                return "no_match:row_eq"
            elif expected_field_count == llm_field_count:
                return "no_match:field_eq"
            else:
                return "no_match:other"

        chart_data: dict[str, int] = {}

        for nm in no_match_details:
            category = parse_details(nm)

            if category not in chart_data:
                chart_data[category] = 1
            else:
                chart_data[category] += 1
        

        chart_cat = []
        chart_val = []
        chart_cat.append("exact_match")
        chart_val.append(exact_match_count)
      
        for k, v in chart_data.items():
            chart_cat.append(k)
            chart_val.append(v)

        chart_cat.append("sql_errors")
        chart_val.append(sql_error_count)

        chart_cat.append("other_errors")
        chart_val.append(total_count - (exact_match_count + no_match_count + sql_error_count))
        
        create_graph(
            output_path=stats_filepath.removesuffix(".stats.json") + ".success_graph.png",
            categories=chart_cat,
            values=chart_val,
            xlabel="Query Results Type",
            ylabel="Occurences",
            title="Generation chart"
        )