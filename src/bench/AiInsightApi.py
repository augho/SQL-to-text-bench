import requests
import threading
import time
import json

from src.bench.BenchInput import BenchInput, ListId
from src.bench.BenchOutput import BenchOutput
from src.lib.utils import human_in_the_loop


class AiInsightApi:
    MAX_RPM: int | None = None

    def __init__(self, hostname: str, port: int) -> None:
        self.hostname: str = hostname
        self.port: int = port

    def test(self, endpoint: str, expected: dict) -> None:
        url = f"http://{self.hostname}:{self.port}" + endpoint

        response = requests.get(url)
        data = json.loads(response.content)
        
        assert type(data) is dict

        for k, v in expected.items():
            assert k in data.keys(), f"response keys: {data.keys()}\nexpected keys: {expected.keys()}"
            assert v == data[k], f"expected value for {k}: {v}\nresponse value: {data[k]}"

        

        
    def ask_agent(self, bench_input: BenchInput, easy_mode: bool = False) -> BenchOutput:
        if easy_mode and bench_input.get_list_id() == ListId.LIST_2:
            raise Exception("No easy question for list 2 inputs")
        
        url = f"http://{self.hostname}:{self.port}/api/v1/agent/ask"
        payload = {
            "prompt": bench_input.easy_question if easy_mode else bench_input.question,
            "conversationId": None
        }
        headers = {"content-type": "application/json"}

        response = requests.post(url, json=payload, headers=headers)

        if response.ok:
            data = json.loads(response.content)
            if not data["requiresApproval"]:
                return BenchOutput(
                    bench_input,
                    data["messageText"],
                    "[ERR1] Agent didn't generate SQL or hasn't mark it as requiring approval"
                )
            try:
                bench_output = BenchOutput(
                    bench_input,
                    data["messageText"],
                    None   
                )
                return bench_output
            except Exception:
                print("[API ERR] Response format", response.json())
                return BenchOutput(
                    bench_input,
                    None,
                    "[ERR2] Response format error"
                )
        else:
            print("[HTTP ERROR]", response.status_code)
            return BenchOutput(
                bench_input,
                None,
                f"[ERR3] HTTP {response.status_code} Error"
            )
    def _ask_agent_wrapper(self, bench_input: BenchInput, easy_mode: bool, return_store: list) -> None:
        result = self.ask_agent(bench_input, easy_mode)
        return_store.append(result)


    def chain_ask(self, bench_inputs: list[BenchInput], easy_mode:bool = False, do_logging: bool = True) -> list[BenchOutput]:
        if AiInsightApi.MAX_RPM is None:
            proceed_confirmation = human_in_the_loop(
                "You haven't set a max RPM for the api calls do you want to proceed (y/n) ?"
            )
            if not proceed_confirmation:
                    if do_logging:
                        print("[LOG] Canceling API calls")
                    return []
            if do_logging: 
                print("[LOG] Proceeding execution!")
        tasks: list[threading.Thread] = []
        bench_outputs: list[BenchOutput] = []
        for bench_input in bench_inputs:
            tasks.append(threading.Thread(target=self._ask_agent_wrapper, args=(bench_input, easy_mode, bench_outputs)))
        
        # To not hit the LLM Api rate limit we add a delay between requests (the +1 is just to make sure)
        thread_delay_seconds: float = (60 + 1) / AiInsightApi.MAX_RPM if AiInsightApi.MAX_RPM is not None else 0
        for i, t in enumerate(tasks):
            if do_logging:
                print(f"Starting generation {i + 1}/{len(tasks)}")
            t.start()
            time.sleep(thread_delay_seconds)

        [t.join() for t in tasks]

        if do_logging:
            print("\n[LOG] SQL GENERATION SUCCESS")

        bench_outputs.sort(key= lambda b: b.matching_input.id)

        return bench_outputs






"""
/agent/ask api response format
{
  "messageId": 0,
  "senderType": "agent",
  "messageText": "SELECT COUNT(EmployeeId) FROM Employee",
  "sentAt": "2025-10-20T10:13:54.9457861Z",
  "requiresApproval": true,
  "approvalStatus": "pending"
}
"""