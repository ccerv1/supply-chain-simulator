from dataclasses import dataclass
from .base import BaseModel

@dataclass
class Actor(BaseModel):
    id: str
    loyalty: float
    country_id: str
    
@dataclass
class Farmer(Actor):
    geography_id: str
    num_plots: int
    production_amount: float

    @classmethod
    def from_dict(cls, data: dict) -> 'Farmer':
        return cls(**data)

@dataclass
class Middleman(Actor):
    competitiveness: float = 0.5

    @classmethod
    def from_dict(cls, data: dict) -> 'Middleman':
        return cls(**data)

@dataclass
class Exporter(Actor):
    competitiveness: float = 0.5
    eu_preference: float = 0.5

    @classmethod
    def from_dict(cls, data: dict) -> 'Exporter':
        return cls(**data)