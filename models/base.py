from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class BaseActor(ABC):
    id: str
    country_id: str
    loyalty: float
    competitiveness: float

    @abstractmethod
    def to_dict(self) -> dict:
        pass

    @classmethod
    @abstractmethod
    def from_dict(cls, data: dict) -> 'BaseActor':
        pass 