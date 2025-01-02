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

logger = logging.getLogger(__name__)

class CountrySimulation:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db = DatabaseManager(db_path)
        self.initializer = CountryInitializer(self.db)
        self.simulator = TradeSimulator(self.db)

    def initialize_country_actors(self, country_id: str):
        """Initialize a country with basic actors if not already present."""
        logger.info(f"Initializing actors for country: {country_id}")
        
        try:
            # Check if country is already initialized
            country = self.initializer.country_registry.get_by_id(country_id)
            if not country:
                logger.info("Creating new country...")
                country = self.initializer.initialize_country(country_id)
            
            return country

        except Exception as e:
            logger.error(f"Error initializing country actors: {str(e)}", exc_info=True)
            raise

    def initialize_middleman_geographies(self, country_id: str):
        """Create initial middleman-geography assignments through trading relationships."""
        logger.info(f"Initializing middleman geographies for country: {country_id}")
        
        try:
            # Get all required actors
            geographies = self.initializer.geography_registry.get_by_country(country_id)
            farmers = self.initializer.farmer_registry.get_all_by_country(country_id)
            middlemen = self.initializer.middleman_registry.get_all()
            exporters = self.initializer.exporter_registry.get_all()
            
            # Check for existing year 0 relationships
            initial_relationships = self.simulator.trading_registry.get_by_year_and_country(0, country_id)
            if not initial_relationships:
                country = self.initializer.country_registry.get_by_id(country_id)
                logger.info("Creating initial trading relationships...")
                initial_relationships = self.simulator.create_initial_relationships(
                    country=country,
                    farmers=farmers,
                    middlemen=middlemen,
                    exporters=exporters,
                    geographies=geographies
                )
                self.simulator.trading_registry.create_many(initial_relationships)
                logger.info(f"Created {len(initial_relationships)} initial relationships")
            
            return initial_relationships

        except Exception as e:
            logger.error(f"Error initializing middleman geographies: {str(e)}", exc_info=True)
            raise

    def update_middleman_geographies(self, country_id: str, year: int, geography_change_rate: float = 0.1):
        """Randomly update which geographies middlemen are assigned to."""
        logger.info(f"Updating middleman geography assignments for year {year}")
        
        try:
            middlemen = self.initializer.middleman_registry.get_all()
            geographies = self.initializer.geography_registry.get_by_country(country_id)
            
            updates = {}
            for middleman in middlemen:
                current_geographies = self.simulator.trading_registry.get_middleman_geographies(
                    year - 1, country_id, middleman.id
                )
                
                # Randomly decide if this middleman will change geographies
                if random.random() < geography_change_rate:
                    available_geographies = [g.id for g in geographies if g.id not in current_geographies]
                    if available_geographies:
                        # Remove one random geography and add another
                        if current_geographies:
                            current_geographies.remove(random.choice(current_geographies))
                        current_geographies.append(random.choice(available_geographies))
                
                updates[middleman.id] = current_geographies
            
            return updates

        except Exception as e:
            logger.error(f"Error updating middleman geographies: {str(e)}", exc_info=True)
            raise

    def simulate_trading_year(self, country_id: str, year: int):
        """Simulate trading relationships for a specific year."""
        logger.info(f"Simulating trading for year {year}")
        
        try:
            # Check if year already simulated
            existing_relationships = self.simulator.trading_registry.get_by_year(year)
            if existing_relationships:
                logger.info(f"Year {year} already simulated.")
                return existing_relationships

            # Get previous year's relationships
            previous_relationships = self.simulator.trading_registry.get_by_year_and_country(year - 1, country_id)
            if not previous_relationships:
                raise ValueError(f"No relationships found for previous year {year-1}")

            # Get all required actors
            country = self.initializer.country_registry.get_by_id(country_id)
            farmers = self.initializer.farmer_registry.get_all_by_country(country_id)
            middlemen = self.initializer.middleman_registry.get_all()
            exporters = self.initializer.exporter_registry.get_all()

            # Create new relationships
            logger.info(f"Creating new relationships for year {year}")
            new_relationships = self.simulator.simulate_next_year(
                previous_relationships=previous_relationships,
                country=country,
                farmers=farmers,
                middlemen=middlemen,
                exporters=exporters,
                year=year
            )
            
            # Persist to database
            logger.info(f"Persisting {len(new_relationships)} relationships")
            self.simulator.trading_registry.create_many(new_relationships)
            
            return new_relationships

        except Exception as e:
            logger.error(f"Error simulating trading year: {str(e)}", exc_info=True)
            raise
