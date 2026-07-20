from dataclasses import dataclass


@dataclass
class Step:
    id: str
    idx: int
    title: str
    file: str