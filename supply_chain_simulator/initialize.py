import numpy as np
import pandas as pd
from pathlib import Path
import json
from typing import List, Dict
from scipy.spatial.distance import cdist
import ast

from models import Country, Farmer, Middleman, Exporter, Geography
from database import DatabaseManager
from trade_sim import create_trading_relationships

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'

try:
    COUNTRY_CODE_MAPPING = json.load(open(DATA_DIR / 'country_codes.json'))
    COUNTRY_ASSUMPTIONS = pd.read_csv(DATA_DIR / 'country_assums.csv')

    # https://enveritas.looker.com/explore/jebena/geo_map_dataset?qid=ggpViZAcxqi85oAkC7Jwyy
    GEOGRAPHY_DATA = pd.read_csv(DATA_DIR / '_local/jebena_geodata.csv')
except FileNotFoundError as e:
    raise FileNotFoundError(f"Required data file not found: {e.filename}")

FARMER_PLOT_THRESHOLDS = {
    'low': {
        'threshold': 0.2,  # Bottom 20%
        'plots': 1
    },
    'high': {
        'threshold': 0.8,  # Top 20%
        'plots': 3
    }
}

LOGNORMAL_ADJUSTMENT = 0.5


def initialize_country(country: Country, db_path: str) -> None:
    """Initialize a country's supply chain actors and store them in SQLite.
    
    Args:
        country: Country object to initialize
        db_path: Path to SQLite database
        
    Raises:
        ValueError: If country ID is not found in mapping
        KeyError: If country assumptions are missing
    """
    if country.id.lower() not in COUNTRY_CODE_MAPPING:
        raise ValueError(f"Country ID {country.id} not found in mapping")
        
    # Load and process geography data
    geo_df = GEOGRAPHY_DATA[GEOGRAPHY_DATA['ssu_name'].str[:2] == country.id.lower()]
    country.geographies = _create_geographies(geo_df)
    
    # Calculate country totals from geographies
    total_farmers = sum(g.num_farmers for g in country.geographies)
    total_production = sum(g.total_production_kg for g in country.geographies)

    # Get additional data from assumptions
    country_assumptions = COUNTRY_ASSUMPTIONS[
        COUNTRY_ASSUMPTIONS['country'] == COUNTRY_CODE_MAPPING[country.id.lower()]['full']
    ].iloc[0]

    # Update country attributes
    country.name = COUNTRY_CODE_MAPPING[country.id.lower()]['full']
    country.total_production = total_production
    country.num_farmers = total_farmers
    country.num_middlemen = country_assumptions['num_middlemen']
    country.num_exporters = country_assumptions['num_exporters']
    country.exports_to_eu = country_assumptions['exports_to_eu']

    # Create supply chain actors
    farmers = []
    for geography in country.geographies:
        farmers.extend(_create_farmers(geography, country))
    
    middlemen = _create_middlemen(country)
    exporters = _create_exporters(country)

    # Create trading relationships
    relationships = create_trading_relationships(
        country=country,
        farmers=farmers,
        middlemen=middlemen,
        exporters=exporters,
        geographies=country.geographies
    )

    # Store everything in the database
    with DatabaseManager(db_path) as db:
        db.initialize_tables()
        db.insert_country(country)
        db.insert_geographies(country.geographies)
        db.insert_farmers(farmers)
        db.insert_middlemen(middlemen)
        db.insert_exporters(exporters)
        db.insert_trading_relationships(relationships)

def _create_geographies(df: pd.DataFrame) -> List[Geography]:
    """Create Geography objects from DataFrame."""
    geographies = []
    for _, row in df.iterrows():

        country_id = row['ssu_name'][:2].upper()

        # Calculate total farmers and production
        total_farmers = row['estimated_arabica_farmer_population'] + row['estimated_robusta_farmer_population']
        total_production = row['estimated_arabica_production_in_kg'] + row['estimated_robusta_production_in_kg']

        if total_farmers < 1 or total_production < 1:
            continue
        
        # Determine primary crop
        arabica_ratio = row['estimated_arabica_production_in_kg'] / total_production if total_production > 0 else 0
        primary_crop = "arabica" if arabica_ratio >= 0.8 else "robusta" if arabica_ratio <= 0.2 else "mixed"
        
        geography = Geography(
            id=row['ssu_name'],
            name=row['label'],
            country=country_id,
            centroid=row['centroid'],
            producing_area_name=row['pa_name'],
            num_farmers=int(total_farmers),
            total_production_kg=int(total_production),
            primary_crop=primary_crop
        )
        geographies.append(geography)
    return geographies

def _create_farmers(geography: Geography, country: Country) -> List[Farmer]:
    """Create Farmer objects for a given geography."""
    if geography.num_farmers == 0 or geography.total_production_kg == 0:
        return []

    # Generate production amounts using lognormal distribution
    production = np.random.lognormal(
        mean=np.log(geography.total_production_kg / geography.num_farmers) - LOGNORMAL_ADJUSTMENT,
        sigma=country.farmer_production_sigma,
        size=geography.num_farmers
    )
    
    # Normalize to match total production
    production = production * (geography.total_production_kg / production.sum())
    production = np.round(production).astype(int)
    
    # Adjust for rounding errors
    diff = geography.total_production_kg - production.sum()
    if diff != 0:
        # Add/subtract the difference from the largest producer
        production[production.argmax()] += diff

    # Calculate percentiles for plot assignment
    percentiles = (production / production.sum()).argsort() / len(production)
    farmers = []
    for i in range(geography.num_farmers):
        if percentiles[i] <= FARMER_PLOT_THRESHOLDS['low']['threshold']:
            num_plots = FARMER_PLOT_THRESHOLDS['low']['plots']
        elif percentiles[i] >= FARMER_PLOT_THRESHOLDS['high']['threshold']:
            num_plots = FARMER_PLOT_THRESHOLDS['high']['plots']
        else:
            # Middle farmers randomly get 1-3 plots
            prob_extra_plots = (
                (percentiles[i] - FARMER_PLOT_THRESHOLDS['low']['threshold'])
                / (FARMER_PLOT_THRESHOLDS['high']['threshold'] - FARMER_PLOT_THRESHOLDS['low']['threshold'])
            )
            num_plots = (
                FARMER_PLOT_THRESHOLDS['low']['plots'] + sum(
                    np.random.random() < prob_extra_plots
                    for _ in range(
                        FARMER_PLOT_THRESHOLDS['high']['plots'] - FARMER_PLOT_THRESHOLDS['low']['plots']
                    )
                )
            )

        # Generate random loyalty score
        loyalty = np.clip(np.random.normal(0.5, 0.15), 0, 1)

        farmer = Farmer(
            id=f"{geography.id}_F{i:06d}",
            geography=geography,
            num_plots=num_plots,
            production_amount=float(production[i]),
            middleman_loyalty=loyalty
        )
        farmers.append(farmer)

    return farmers

def _create_middlemen(country: Country) -> List[Middleman]:
    """Create Middleman objects for a country."""
    # Generate competitiveness using lognormal distribution
    competitiveness = np.random.lognormal(
        mean=-LOGNORMAL_ADJUSTMENT * country.middleman_capacity_sigma**2,  # Ensures mean of 1
        sigma=country.middleman_capacity_sigma,
        size=country.num_middlemen
    )
    competitiveness /= competitiveness.sum()  # Normalize to sum to 1
    
    # Generate uniform loyalty scores
    farmer_loyalty = np.random.uniform(0, 1, size=country.num_middlemen)
    exporter_loyalty = np.random.uniform(0, 1, size=country.num_middlemen)
    
    middlemen = []
    for i in range(country.num_middlemen):
        middleman = Middleman(
            id=f"{country.id}_M{i:06d}",
            competitiveness=float(competitiveness[i]),
            farmer_loyalty=float(farmer_loyalty[i]),
            exporter_loyalty=float(exporter_loyalty[i])
        )
        middlemen.append(middleman)
    
    return middlemen

def _create_exporters(country: Country) -> List[Exporter]:
    """Create Exporter objects for a country."""
    # Generate competitiveness using Pareto distribution
    competitiveness = 1 + np.random.pareto(
        country.exporter_pareto_alpha, 
        size=country.num_exporters
    )
    competitiveness /= competitiveness.sum()  # Normalize to sum to 1
    
    # Generate uniform EU preference and loyalty scores
    eu_preference = np.random.uniform(0, 1, size=country.num_exporters)
    middleman_loyalty = np.random.uniform(0, 1, size=country.num_exporters)
    
    exporters = []
    for i in range(country.num_exporters):
        exporter = Exporter(
            id=f"{country.id}_E{i:06d}",
            competitiveness=float(competitiveness[i]),
            eu_preference=float(eu_preference[i]),
            middleman_loyalty=float(middleman_loyalty[i])
        )
        exporters.append(exporter)
    
    return exporters
