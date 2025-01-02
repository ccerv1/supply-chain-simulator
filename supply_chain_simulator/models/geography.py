from dataclasses import dataclass
from typing import Optional, Tuple
from .base import BaseModel

@dataclass
class Country(BaseModel):
    id: str
    name: str = "Default"
    total_production: int = 10_000_000 * 60
    num_farmers: int = 500_000
    num_middlemen: int = 1000
    num_exporters: int = 100
    max_buyers_per_farmer: int = 3
    max_exporters_per_middleman: int = 3
    farmer_production_sigma: float = 0.8
    middleman_capacity_sigma: float = 0.5
    exporter_pareto_alpha: float = 1.16
    farmer_switch_rate: float = 0.20
    middleman_switch_rate: float = 0.30
    exports_to_eu: int = 4_000_000
    traceability_rate: float = 0.9

    @classmethod
    def from_dict(cls, data: dict) -> 'Country':
        return cls(**data)

@dataclass
class Geography(BaseModel):
    id: str
    name: str
    country_id: str
    centroid: str
    producing_area_name: str
    num_farmers: int = 0
    total_production_kg: int = 0
    primary_crop: str = "arabica"

    @classmethod
    def from_dict(cls, data: dict) -> 'Geography':
        return cls(**data)