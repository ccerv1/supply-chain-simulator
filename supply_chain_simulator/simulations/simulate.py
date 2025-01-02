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

logger = logging.getLogger(__name__)

class CountrySimulation:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db = DatabaseManager(db_path)
        self.initializer = CountryInitializer(self.db)
        self.simulator = TradeSimulator(self.db)
        self.country_id = None
        self.middleman_geographies = {}
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
                # Initial assignment - use the imported function
                geo_to_middlemen = assign_middlemen_to_geographies(geographies, middlemen)
                updates = {m.id: [] for m in middlemen}
                for geo_id, mm_list in geo_to_middlemen.items():
                    for mm in mm_list:
                        updates[mm.id].append(geo_id)
            else:
                # Update based on previous year
                updates = {}
                for middleman in middlemen:
                    current_geos = self.simulator.trading_registry.get_middleman_geographies(
                        year - 1, self.country_id, middleman.id
                    )
                    
                    if random.random() < geography_change_rate:
                        # Randomly change one geography
                        available = [g.id for g in geographies if g.id not in current_geos]
                        if available and current_geos:
                            current_geos.remove(random.choice(current_geos))
                            current_geos.append(random.choice(available))
                    
                    updates[middleman.id] = current_geos

            # Check that every geography has at least one middleman
            covered_geos = set()
            for mm_id, geo_list in updates.items():
                covered_geos.update(geo_list)

            uncovered_geos = set(g.id for g in geographies) - covered_geos
            
            if uncovered_geos and year > 0:
                # Get previous year assignments for uncovered geographies
                for geo_id in uncovered_geos:
                    prev_assignments = self.simulator.trading_registry.get_middleman_geographies(
                        year - 1, self.country_id, geo_id
                    )
                    if prev_assignments:
                        # Assign to first middleman from previous year
                        updates[prev_assignments[0]].append(geo_id)
            
            self.middleman_geographies = updates
            return updates

        except Exception as e:
            logger.error(f"Error setting middleman geographies: {str(e)}", exc_info=True)
            raise

    def simulate_trading_year(self, year: int):
        """Simulate trading relationships for a specific year."""
        logger.info(f"Simulating trading for year {year}")
        
        try:
            # Check for existing relationships
            existing = self.simulator.trading_registry.get_by_year_and_country(year, self.country_id)
            if existing:
                logger.info(f"Year {year} already simulated.")
                return existing

            # Get actors
            country = self.initializer.country_registry.get_by_id(self.country_id)
            farmers = self.initializer.farmer_registry.get_all_by_country(self.country_id)
            middlemen = self.initializer.middleman_registry.get_by_country(self.country_id)
            exporters = self.initializer.exporter_registry.get_by_country(self.country_id)
            
            # Use the already set middleman geographies
            if not self.middleman_geographies:
                raise ValueError("Middleman geographies not set. Call set_middleman_geographies first.")

            # Generate trade flows
            trade_flows = self.simulator.simulate_trade_flows(
                country=country,
                farmers=farmers,
                middlemen=middlemen,
                exporters=exporters,
                middleman_geographies=self.middleman_geographies,
                year=year
            )
            
            # Save to database
            self.simulator.trading_registry.create_many(trade_flows)
            return trade_flows

        except Exception as e:
            logger.error(f"Error simulating trading year: {str(e)}", exc_info=True)
            raise
