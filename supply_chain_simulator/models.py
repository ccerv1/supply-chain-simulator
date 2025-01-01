from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple


@dataclass
class Country:
    id: str = "XX" # two letter code
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
    traceability_rate: float = 0.5


@dataclass
class Geography:
    id: str  # ssu_name
    name: str # label
    country: Country 
    centroid: Tuple[float, float]
    producing_area_name: str # pa_name
    num_farmers: int = 0 # total arabica + robusta farmers
    total_production_kg: int = 0 # total arabica + robusta production
    primary_crop: str = "arabica" # "arabica" or "robusta" (if neither is more than 80%, then "mixed")


@dataclass
class Exporter:
    id: str
    competitiveness: float
    eu_preference: float
    middleman_loyalty: float


@dataclass
class Middleman:
    id: str
    competitiveness: float
    farmer_loyalty: float
    exporter_loyalty: float


@dataclass
class Farmer:
    id: str
    geography: Geography
    num_plots: int
    production_amount: float
    middleman_loyalty: float
