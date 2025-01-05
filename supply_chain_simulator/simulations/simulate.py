"""
Coffee Supply Chain Simulator
Core simulation logic for country-level coffee supply chains
"""

import logging
import random
import numpy as np
from typing import List, Dict

from database.manager import DatabaseManager
from models.actors import Farmer, Middleman, Exporter
from models.geography import Country, Geography
from simulations.initialize import CountryInitializer
from simulations.trade import TradeSimulator
from simulations.middleman_geographies import assign_middlemen_to_geographies
from config.simulation import DEFAULT_RANDOM_SEED
from database.registries import (
    MiddlemanGeographyRegistry, 
    FarmerMiddlemanRegistry, 
    MiddlemanExporterRegistry
)

logger = logging.getLogger(__name__)

class CountrySimulation:
    """Manages simulation steps for a single country."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.initializer = CountryInitializer(self.db)
        self.simulator = TradeSimulator(self.db)
        self.country_id = None
        
        # Relationship registries
        self.mm_geo_registry = MiddlemanGeographyRegistry(self.db)
        self.farmer_mm_registry = FarmerMiddlemanRegistry(self.db)
        self.mm_exp_registry = MiddlemanExporterRegistry(self.db)
        
        random.seed(DEFAULT_RANDOM_SEED)

    def initialize_country_actors(self, country_id: str) -> None:
        """Initialize or verify a country's actors exist."""
        try:
            country = self.initializer.country_registry.get_by_id(country_id)
            if not country:
                logger.info(f"Creating new country: {country_id}")
                country = self.initializer.initialize_country(country_id)
            self.country_id = country_id
            
        except Exception as e:
            logger.error(f"Error initializing country actors: {str(e)}")
            raise

    def set_middleman_geographies(self, year: int, geography_change_rate: float = 0.1) -> List[Dict]:
        """Assign or update middleman-geography relationships."""
        try:
            # Get actors
            middlemen = self.initializer.middleman_registry.get_by_country(self.country_id)
            geographies = self.initializer.geography_registry.get_by_country(self.country_id)
            
            # Initial assignment for year 0
            if year == 0:
                geo_assignments = assign_middlemen_to_geographies(geographies, middlemen)
                relationships = [
                    {'middleman_id': mm.id, 'geography_id': geo_id}
                    for geo_id, mm_list in geo_assignments.items()
                    for mm in mm_list
                ]
                self.mm_geo_registry.create_many(relationships, year)
                return relationships
            
            # Update existing relationships
            current_rels = self.mm_geo_registry.get_active_relationships(year, self.country_id)
            self._update_geography_relationships(
                current_rels, 
                middlemen, 
                geographies, 
                year, 
                geography_change_rate
            )
            return self.mm_geo_registry.get_active_relationships(year, self.country_id)
            
        except Exception as e:
            logger.error(f"Error setting middleman geographies: {str(e)}")
            raise

    def simulate_trading_year(self, year: int) -> None:
        """Run trading simulation for a specific year."""
        try:
            # Check if already simulated
            existing = self.simulator.trading_registry.get_by_year_and_country(year, self.country_id)
            if existing:
                logger.info(f"Year {year} already simulated for {self.country_id}")
                return

            # Get actors
            country = self.initializer.country_registry.get_by_id(self.country_id)
            farmers = self.initializer.farmer_registry.get_all_by_country(self.country_id)
            middlemen = self.initializer.middleman_registry.get_by_country(self.country_id)
            exporters = self.initializer.exporter_registry.get_by_country(self.country_id)
            
            # Verify geography relationships exist
            mm_geo_rels = self.mm_geo_registry.get_active_relationships(year, self.country_id)
            if not mm_geo_rels:
                raise ValueError("No middleman-geography relationships found")

            # Generate and save trade flows
            trade_flows = self.simulator.simulate_trade_flows(
                country=country,
                farmers=farmers,
                middlemen=middlemen,
                exporters=exporters,
                year=year
            )
            
            if trade_flows:
                self.simulator.trading_registry.create_many(trade_flows)
            
        except Exception as e:
            logger.error(f"Error simulating trading year: {str(e)}")
            raise

    def _update_geography_relationships(self, current_rels: List[Dict], 
                                     middlemen: List[Middleman],
                                     geographies: List[Geography],
                                     year: int,
                                     change_rate: float) -> None:
        """Helper method to update geography relationships."""
        relationships_to_end = []
        new_relationships = []
        
        for middleman in middlemen:
            if random.random() < change_rate:
                # Get current and available geographies
                current_geos = [
                    rel['geography_id'] for rel in current_rels 
                    if rel['middleman_id'] == middleman.id
                ]
                
                if current_geos:
                    # End one relationship
                    old_geo = random.choice(current_geos)
                    relationships_to_end.append((middleman.id, old_geo))
                    
                    # Start a new relationship
                    available_geos = [g.id for g in geographies if g.id not in current_geos]
                    if available_geos:
                        new_geo = random.choice(available_geos)
                        new_relationships.append({
                            'middleman_id': middleman.id,
                            'geography_id': new_geo
                        })
        
        # Apply changes
        if relationships_to_end:
            self.mm_geo_registry.end_relationships(relationships_to_end, year)
        if new_relationships:
            self.mm_geo_registry.create_many(new_relationships, year)
