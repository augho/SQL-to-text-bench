from __future__ import annotations
from enum import Enum

class BenchInput:
    def __init__(self, id: int, easy_question: str|None, question: str, sql: str) -> None:
        self.id: int = id
        self.easy_question: str|None = easy_question
        self.question: str = question
        self.sql: str = sql
    
    
    @staticmethod
    def init_from_json(json_item: dict) -> BenchInput:
        try:
            id = json_item['id']
            easy_question =  None if 'easy_question' not in json_item.keys() \
                else json_item['easy_question']
            question = json_item['question']
            sql = json_item['sql']
            return BenchInput(id, easy_question, question, sql)
        except KeyError:
            raise KeyError("[ERROR] Json malformed", json_item)
        

    def get_list_id(self) -> ListId:
        return ListId.LIST_2 if self.easy_question is None else ListId.LIST_1


    def __str__(self) -> str:
        res = f"----- INPUT n°{self.id} -----\n"
        if self.get_list_id() == ListId.LIST_1:
            res += f"easy_q: {self.easy_question}\n"
        res += f"q     : {self.question}\n"
        res += f"sql   : {self.sql}\n" 

        return res



class ListId(Enum):
    """First list which has an easy question and a question"""
    LIST_1 = 1
    """Second list which has only one question"""
    LIST_2 = 2