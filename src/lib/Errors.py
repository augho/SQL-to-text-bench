from dataclasses import dataclass

@dataclass
class AiApiError:
    code: int
    message: str
    details: str