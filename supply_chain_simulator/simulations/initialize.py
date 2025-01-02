import numpy as np
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
import json
import ast

from models.actors import Farmer, Middleman, Exporter
from models.geography import Country, Geography
from database.manager import DatabaseManager
from database.registries import (
    CountryRegistry, 
    GeographyRegistry, 
    FarmerRegistry, 
    MiddlemanRegistry, 
    ExporterRegistry,
    TradingRegistry
)
from config.settings import DATA_DIR
from config.simulation import FARMER_PLOT_THRESHOLDS, LOGNORMAL_ADJUSTMENT

class CountryInitializer:
    """Handles initialization of a country's supply chain actors."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.country_registry = CountryRegistry(db_manager)
        self.geography_registry = GeographyRegistry(db_manager)
        self.farmer_registry = FarmerRegistry(db_manager)
        self.middleman_registry = MiddlemanRegistry(db_manager)
        self.exporter_registry = ExporterRegistry(db_manager)
        self.trading_registry = TradingRegistry(db_manager)
        
        # Load reference data
        self.country_codes = self._load_country_codes()
        self.country_assumptions = self._load_country_assumptions()
        self.geography_data = self._load_geography_data()

    def initialize_country(self, country_id: str) -> Country:
        """Initialize a country's supply chain actors and store in database."""
        if country_id.lower() not in self.country_codes:
            raise ValueError(f"Country ID {country_id} not found in mapping")

        # Create and configure country
        country = self._create_country(country_id)
        
        # Create geographies
        geographies = self._create_geographies(country)
        
        # Create supply chain actors
        farmers = self._create_farmers(country, geographies)
        middlemen = self._create_middlemen(country)
        exporters = self._create_exporters(country)
        
        # Store everything in database
        self._save_to_database(country, geographies, farmers, middlemen, exporters)
        
        return country

    def _load_country_codes(self) -> Dict:
        """Load country code mappings."""
        with open(DATA_DIR / 'country_codes.json') as f:
            return json.load(f)

    def _load_country_assumptions(self) -> pd.DataFrame:
        """Load country assumptions data."""
        return pd.read_csv(DATA_DIR / 'country_assums.csv')

    def _load_geography_data(self) -> pd.DataFrame:
        """Load geography data."""
        return pd.read_csv(DATA_DIR / '_local/jebena_geodata.csv')

    def _create_country(self, country_id: str) -> Country:
        """Create and configure a country instance."""
        country_name = self.country_codes[country_id.lower()]['full']
        assumptions = self.country_assumptions[
            self.country_assumptions['country'] == country_name
        ].iloc[0]
        
        # Calculate totals from geography data first
        geo_df = self.geography_data[
            self.geography_data['ssu_name'].str[:2] == country_id.lower()
        ]
        
        total_farmers = int(geo_df['estimated_arabica_farmer_population'].sum() + 
                           geo_df['estimated_robusta_farmer_population'].sum())
        total_production = int(geo_df['estimated_arabica_production_in_kg'].sum() + 
                             geo_df['estimated_robusta_production_in_kg'].sum())
        
        return Country(
            id=country_id,
            name=country_name,
            num_farmers=total_farmers,
            total_production=total_production,
            num_middlemen=int(assumptions['num_middlemen']),
            num_exporters=int(assumptions['num_exporters']),
            exports_to_eu=int(assumptions['exports_to_eu'])
        )

    def _create_geographies(self, country: Country) -> List[Geography]:
        """Create geography objects for a country."""
        geo_df = self.geography_data[
            self.geography_data['ssu_name'].str[:2] == country.id.lower()
        ]
        
        geographies = []
        for _, row in geo_df.iterrows():
            total_farmers = (
                row['estimated_arabica_farmer_population'] + 
                row['estimated_robusta_farmer_population']
            )
            total_production = (
                row['estimated_arabica_production_in_kg'] + 
                row['estimated_robusta_production_in_kg']
            )
            
            if total_farmers < 1 or total_production < 1:
                continue
            
            # Determine primary crop
            arabica_ratio = (
                row['estimated_arabica_production_in_kg'] / total_production 
                if total_production > 0 else 0
            )
            primary_crop = (
                "arabica" if arabica_ratio >= 0.8 
                else "robusta" if arabica_ratio <= 0.2 
                else "mixed"
            )
            
            geography = Geography(
                id=row['ssu_name'],
                name=row['label'],
                country_id=country.id,
                centroid=row['centroid'],
                producing_area_name=row['pa_name'],
                num_farmers=int(total_farmers),
                total_production_kg=int(total_production),
                primary_crop=primary_crop
            )
            geographies.append(geography)
        
        return geographies

    def _create_farmers(
        self, 
        country: Country, 
        geographies: List[Geography]
    ) -> List[Farmer]:
        """Create farmer objects for all geographies."""
        farmers = []
        for geography in geographies:
            farmers.extend(self._create_farmers_for_geography(country, geography))
        return farmers

    def _create_farmers_for_geography(
        self, 
        country: Country, 
        geography: Geography
    ) -> List[Farmer]:
        """Create farmer objects for a specific geography."""
        if geography.num_farmers == 0 or geography.total_production_kg == 0:
            return []

        # Generate production amounts
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
            production[production.argmax()] += diff

        # Calculate percentiles for plot assignment
        percentiles = (production / production.sum()).argsort() / len(production)
        
        farmers = []
        for i in range(geography.num_farmers):
            num_plots = self._calculate_num_plots(percentiles[i])
            loyalty = np.clip(np.random.normal(0.5, 0.25), 0, 1)
            
            farmer = Farmer(
                id=f"{geography.id}_F{i:06d}",
                geography_id=geography.id,
                country_id=country.id,
                num_plots=int(num_plots),
                production_amount=float(production[i]),
                loyalty=float(loyalty)
            )
            farmers.append(farmer)

        return farmers

    def _calculate_num_plots(self, percentile: float) -> int:
        """Calculate number of plots based on farmer's production percentile."""
        if percentile <= FARMER_PLOT_THRESHOLDS['low']['threshold']:
            return FARMER_PLOT_THRESHOLDS['low']['plots']
        elif percentile >= FARMER_PLOT_THRESHOLDS['high']['threshold']:
            return FARMER_PLOT_THRESHOLDS['high']['plots']
        else:
            prob_extra_plots = (
                (percentile - FARMER_PLOT_THRESHOLDS['low']['threshold']) /
                (FARMER_PLOT_THRESHOLDS['high']['threshold'] - FARMER_PLOT_THRESHOLDS['low']['threshold'])
            )
            return FARMER_PLOT_THRESHOLDS['low']['plots'] + sum(
                np.random.random() < prob_extra_plots
                for _ in range(
                    FARMER_PLOT_THRESHOLDS['high']['plots'] - FARMER_PLOT_THRESHOLDS['low']['plots']
                )
            )

    def _create_middlemen(self, country: Country) -> List[Middleman]:
        """Create middleman objects for a country."""
        competitiveness = np.random.lognormal(
            mean=-LOGNORMAL_ADJUSTMENT * country.middleman_capacity_sigma**2,
            sigma=country.middleman_capacity_sigma,
            size=country.num_middlemen
        )
        competitiveness /= competitiveness.sum()
        loyalty = np.random.uniform(0, 1, size=country.num_middlemen)
        
        return [
            Middleman(
                id=f"{country.id}_M{i:06d}",
                country_id=country.id,
                competitiveness=float(competitiveness[i]),
                loyalty=float(loyalty[i])
            )
            for i in range(country.num_middlemen)
        ]

    def _create_exporters(self, country: Country) -> List[Exporter]:
        """Create exporter objects for a country."""
        competitiveness = 1 + np.random.pareto(
            country.exporter_pareto_alpha, 
            size=country.num_exporters
        )
        competitiveness /= competitiveness.sum()
        
        eu_preference = np.random.uniform(0, 1, size=country.num_exporters)
        loyalty = np.random.uniform(0, 1, size=country.num_exporters)
        
        return [
            Exporter(
                id=f"{country.id}_E{i:06d}",
                country_id=country.id,
                competitiveness=float(competitiveness[i]),
                eu_preference=float(eu_preference[i]),
                loyalty=float(loyalty[i])
            )
            for i in range(country.num_exporters)
        ]

    def _save_to_database(
        self,
        country: Country,
        geographies: List[Geography],
        farmers: List[Farmer],
        middlemen: List[Middleman],
        exporters: List[Exporter]
    ) -> None:
        """Save all created objects to the database."""
        self.country_registry.create(country)
        self.geography_registry.create_many(geographies)
        self.farmer_registry.create_many(farmers)
        self.middleman_registry.create_many(middlemen)
        self.exporter_registry.create_many(exporters)