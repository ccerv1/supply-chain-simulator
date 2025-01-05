import numpy as np
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
import json
import ast
import logging

from models.actors import Farmer, Middleman, Exporter
from models.geography import Country, Geography
from database.manager import DatabaseManager
from database.registries import (
    CountryRegistry, 
    GeographyRegistry, 
    FarmerRegistry, 
    MiddlemanRegistry, 
    ExporterRegistry
)
from config.settings import DATA_DIR
from config.simulation import (
    DEFAULT_RANDOM_SEED,
    LOGNORMAL_ADJUSTMENT,
    FARMER_PLOT_THRESHOLDS
)

logger = logging.getLogger(__name__)

class CountryData:
    """Container for country initialization data"""
    def __init__(self):
        self.country_codes = self._load_country_codes()
        self.country_assumptions = self._load_country_assumptions()
        self.geography_data = self._load_geography_data()

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

class CountryInitializer:
    """Handles initialization of a country's supply chain actors."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        
        # Setup registries
        self.country_registry = CountryRegistry(db_manager)
        self.geography_registry = GeographyRegistry(db_manager)
        self.farmer_registry = FarmerRegistry(db_manager)
        self.middleman_registry = MiddlemanRegistry(db_manager)
        self.exporter_registry = ExporterRegistry(db_manager)
        
        # Load reference data once during initialization
        logger.info("Loading reference data...")
        self.data = CountryData()  # Create data container
        self.country_codes = self.data.country_codes
        self.country_assumptions = self.data.country_assumptions
        self.geography_data = self.data.geography_data
        logger.info("Reference data loaded successfully")

        np.random.seed(DEFAULT_RANDOM_SEED)

    def initialize_country(self, country_id: str) -> Country:
        """Initialize a country with clearer steps."""
        try:
            # 1. Create base entities
            country = self._create_country(country_id)
            geographies = self._create_geographies(country)
            
            # 2. Save country and geographies first
            logger.info(f"Saving country: {country.id} - {country.name}")
            self.country_registry.create(country)
            
            logger.info(f"Saving {len(geographies)} geographies")
            self.geography_registry.create_many(geographies)
            
            # 3. Create and save actors
            logger.info("Creating actors...")
            farmers_count = self._create_farmers(country, geographies)
            logger.info(f"Created {farmers_count} farmers")
            
            middlemen = self._create_middlemen(country)
            logger.info(f"Saving {len(middlemen)} middlemen")
            self.middleman_registry.create_many(middlemen)
            
            exporters = self._create_exporters(country)
            logger.info(f"Saving {len(exporters)} exporters")
            self.exporter_registry.create_many(exporters)
            
            return country
                
        except Exception as e:
            logger.error(f"Failed to initialize {country_id}: {e}")
            raise

    def _create_actors(self, country: Country, geographies: List[Geography]) -> Dict:
        """Create all actors in one place."""
        return {
            'farmers': self._create_farmers(country, geographies),
            'middlemen': self._create_middlemen(country),
            'exporters': self._create_exporters(country)
        }

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
        
        # Use BIGINT for large numbers
        total_farmers = int(min(
            geo_df['estimated_arabica_farmer_population'].sum() + 
            geo_df['estimated_robusta_farmer_population'].sum(),
            2147483647  # PostgreSQL integer max
        ))
        
        total_production = int(min(
            geo_df['estimated_arabica_production_in_kg'].sum() + 
            geo_df['estimated_robusta_production_in_kg'].sum(),
            2147483647  # PostgreSQL integer max
        ))
        
        # Log the actual values for debugging
        logger.info(f"Country {country_id} totals:")
        logger.info(f"- Raw total farmers: {total_farmers:,}")
        logger.info(f"- Raw total production: {total_production:,} kg")
        
        return Country(
            id=country_id,
            name=country_name,
            num_farmers=total_farmers,
            total_production=total_production,
            num_middlemen=int(min(assumptions['num_middlemen'], 2147483647)),
            num_exporters=int(min(assumptions['num_exporters'], 2147483647)),
            exports_to_eu=int(min(assumptions['exports_to_eu'], 2147483647))
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

    def _create_farmers(self, country: Country, geographies: List[Geography]) -> List[Farmer]:
        """Create farmer objects in a memory-efficient, chunked manner."""
        chunk_size = 50_000  # Adjust based on available memory
        farmer_counter = 0
        
        for geography in geographies:
            if geography.num_farmers == 0 or geography.total_production_kg == 0:
                continue
            
            # Generate production values for this geography's farmers
            production = np.random.lognormal(
                mean=np.log(geography.total_production_kg / geography.num_farmers) - LOGNORMAL_ADJUSTMENT,
                sigma=country.farmer_production_sigma,
                size=geography.num_farmers
            )
            
            # Normalize and handle rounding
            production = production * (geography.total_production_kg / production.sum())
            production = np.round(production).astype(int)
            diff = geography.total_production_kg - production.sum()
            if diff != 0:
                production[production.argmax()] += diff

            # Calculate percentiles
            percentiles = (production / production.sum()).argsort() / len(production)
            
            # Process farmers in chunks
            farmers_chunk = []
            for i in range(geography.num_farmers):
                num_plots = self._calculate_num_plots(percentiles[i])
                loyalty = np.clip(np.random.normal(0.5, 0.25), 0, 1)
                
                farmer = Farmer(
                    id=f"{country.id}_F_{farmer_counter:07d}",
                    geography_id=geography.id,
                    country_id=country.id,
                    num_plots=int(num_plots),
                    production_amount=float(production[i]),
                    loyalty=float(loyalty)
                )
                farmers_chunk.append(farmer)
                farmer_counter += 1
                
                # Save chunk when it reaches the size limit
                if len(farmers_chunk) >= chunk_size:
                    self.farmer_registry.create_many(farmers_chunk)
                    farmers_chunk = []
            
            # Save any remaining farmers in the final chunk
            if farmers_chunk:
                self.farmer_registry.create_many(farmers_chunk)

        return farmer_counter  # Return total number of farmers created

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
                id=f"{country.id}_M_{i:05d}",
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
                id=f"{country.id}_E_{i:05d}",
                country_id=country.id,
                competitiveness=float(competitiveness[i]),
                eu_preference=float(eu_preference[i]),
                loyalty=float(loyalty[i])
            )
            for i in range(country.num_exporters)
        ]

    def _save_all(self, country: Country, geographies: List[Geography], actors: Dict) -> None:
        """Save all entities in a single transaction."""
        try:
            with self.db.transaction():
                logger.info(f"Saving country: {country.id} - {country.name}")
                self.country_registry.create(country)
                
                logger.info(f"Saving {len(geographies)} geographies")
                self.geography_registry.create_many(geographies)
                
                # Farmers are already saved in chunks during creation
                logger.info(f"Farmers already saved during chunked creation")
                
                logger.info(f"Saving {len(actors['middlemen'])} middlemen")
                self.middleman_registry.create_many(actors['middlemen'])
                
                logger.info(f"Saving {len(actors['exporters'])} exporters")
                self.exporter_registry.create_many(actors['exporters'])
                
        except Exception as e:
            logger.error(f"Error saving data: {str(e)}")
            raise

    def wipe_country(self, country_id: str) -> None:
        """Remove all data for a specific country."""
        self.db.wipe_country(country_id)