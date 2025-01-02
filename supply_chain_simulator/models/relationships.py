from dataclasses import dataclass
from typing import Optional
from .base import BaseModel

@dataclass
class Trade(BaseModel):
    year: int
    country_id: str
    farmer_id: str
    middleman_id: str
    exporter_id: str
    amount_kg: int
    sold_to_eu: bool
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Trade':
        return cls(**data)
    
    def to_dict(self) -> dict:
        return {
            'year': self.year,
            'country_id': self.country_id,
            'farmer_id': self.farmer_id,
            'middleman_id': self.middleman_id,
            'exporter_id': self.exporter_id,
            'amount_kg': self.amount_kg,
            'sold_to_eu': self.sold_to_eu
        }