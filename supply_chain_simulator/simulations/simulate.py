"""
Coffee Supply Chain Simulator
Unified simulation logic for country-level coffee supply chains
"""

import logging
import random
from pathlib import Path
from typing import List, Dict
from database.manager import DatabaseManager
from simulations.initialize import CountryInitializer
from simulations.trade import TradeSimulator
from simulations.middleman_geographies import assign_middlemen_to_geographies
from config.simulation import DEFAULT_RANDOM_SEED
from database.registries import MiddlemanGeographyRegistry, FarmerMiddlemanRegistry, MiddlemanExporterRegistry

logger = logging.getLogger(__name__)

class CountrySimulation:
    def __init__(self, db_manager: DatabaseManager):
        """Initialize simulation with database manager."""
        self.db = db_manager
        self.initializer = CountryInitializer(self.db)
        self.simulator = TradeSimulator(self.db)
        self.country_id = None
        
        # Add new relationship registries
        self.mm_geo_registry = MiddlemanGeographyRegistry(db_manager)
        self.farmer_mm_registry = FarmerMiddlemanRegistry(db_manager)
        self.mm_exp_registry = MiddlemanExporterRegistry(db_manager)
        
        random.seed(DEFAULT_RANDOM_SEED)

    def initialize_country_actors(self, country_id: str):
        """Initialize a country with basic actors if not already present."""
        
        try:
            # Check if country exists
            country = self.initializer.country_registry.get_by_id(country_id)
            if not country:
                logger.info("Creating new country...")
                country = self.initializer.initialize_country(country_id)
            self.country_id = country_id

        except Exception as e:
            logger.error(f"Error initializing country actors: {str(e)}", exc_info=True)
            raise

    def set_middleman_geographies(self, year: int, geography_change_rate: float = 0.1):
        """Set or update middleman-geography assignments."""
        if not self.country_id:
            raise ValueError("Country ID not set. Call initialize_country_actors first.")
        
        try:
            middlemen = self.initializer.middleman_registry.get_by_country(self.country_id)
            geographies = self.initializer.geography_registry.get_by_country(self.country_id)
            
            if year == 0:
                # Initial assignment
                geo_to_middlemen = assign_middlemen_to_geographies(geographies, middlemen)
                # Convert to list of relationship dictionaries
                relationships = [
                    {
                        'middleman_id': mm.id,
                        'geography_id': geo_id
                    }
                    for geo_id, mm_list in geo_to_middlemen.items()
                    for mm in mm_list
                ]
                self.mm_geo_registry.create_many(relationships, year)
            else:
                # Get current relationships
                current_relationships = self.mm_geo_registry.get_active_relationships(year, self.country_id)
                
                # Process changes based on geography_change_rate
                relationships_to_end = []
                new_relationships = []
                
                for middleman in middlemen:
                    current_geos = [
                        rel['geography_id'] for rel in current_relationships 
                        if rel['middleman_id'] == middleman.id
                    ]
                    
                    if random.random() < geography_change_rate and current_geos:
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
            
            # Return active relationships for this year
            return self.mm_geo_registry.get_active_relationships(year, self.country_id)
            
        except Exception as e:
            logger.error(f"Error setting middleman geographies: {str(e)}")
            raise

    def simulate_trading_year(self, year: int):
        """Simulate trading relationships for a specific year."""
        try:
            # Get actors
            country = self.initializer.country_registry.get_by_id(self.country_id)
            farmers = self.initializer.farmer_registry.get_all_by_country(self.country_id)
            middlemen = self.initializer.middleman_registry.get_by_country(self.country_id)
            exporters = self.initializer.exporter_registry.get_by_country(self.country_id)
            
            # Get active relationships for this year
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
            
            return trade_flows

        except Exception as e:
            logger.error(f"Error simulating trading year: {str(e)}")
            raise

    def simulate_year(self, country_id: str, year: int) -> None:
        """Run single year simulation."""
        try:
            # Set country ID if not already set
            if not self.country_id:
                self.country_id = country_id
            
            # Check existing simulation
            existing = self.simulator.trading_registry.get_by_year_and_country(year, country_id)
            if existing:
                logger.info(f"Year {year} already simulated for {country_id}")
                return

            # Run simulation steps
            logger.info(f"Simulating trading for year {year}")
            
            # 1. Set up geography relationships
            self.set_middleman_geographies(year)
            
            # 2. Run trading simulation
            self.simulate_trading_year(year)
            
        except Exception as e:
            logger.error(f"Error simulating year {year} for {country_id}: {str(e)}")
            raise
