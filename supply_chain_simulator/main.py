"""
Coffee Supply Chain Simulator
Main execution script
"""

import logging
import argparse
from typing import Dict
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

from database.manager import DatabaseManager
from simulations.simulate import CountrySimulation
from simulations.initialize import CountryInitializer
from config.config import DB_CONFIG
from config.simulation import COUNTRIES, NUM_YEARS
from simulations.trade import TradeSimulator

logger = logging.getLogger(__name__)

def ensure_database_initialized(db_manager: DatabaseManager) -> None:
    """Ensure database tables exist."""
    try:
        logger.info("Checking database structure...")
        db_manager.initialize_database()
        logger.info("Database structure verified.")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

def initialize_missing_countries(db_manager: DatabaseManager) -> None:
    """Initialize any countries from COUNTRIES that aren't in the database."""
    initializer = CountryInitializer(db_manager)
    
    # Pre-check which countries need initialization
    existing_countries = {
        country.id for country in initializer.country_registry.get_all()
    }
    countries_to_init = [
        country_id for country_id in COUNTRIES 
        if country_id not in existing_countries
    ]
    
    if not countries_to_init:
        logger.info("All countries already initialized")
        return
        
    logger.info(f"Initializing {len(countries_to_init)} countries: {', '.join(countries_to_init)}")
    
    for country_id in countries_to_init:
        try:
            # Initialize country and its dependencies
            logger.info(f"Initializing new country: {country_id}")
            initializer.initialize_country(country_id)
            
        except Exception as e:
            logger.error(f"Error initializing country {country_id}: {str(e)}")
            continue

def wipe_trading_tables(db_manager: DatabaseManager) -> None:
    """Wipe only the trading-related tables, preserving initialization data."""
    tables_to_wipe = [
        'trading_flows',
        'farmer_middleman_relationships',
        'middleman_exporter_relationships',
        'middleman_geography_relationships'
    ]
    
    logger.info("Wiping trading tables...")
    with db_manager.get_connection() as conn:
        with conn.cursor() as cur:
            for table in tables_to_wipe:
                cur.execute(f'TRUNCATE TABLE {table} CASCADE')
                logger.info(f"Wiped {table}")
            conn.commit()
    logger.info("Trading tables wiped successfully")

def run_test_simulation(db_manager: DatabaseManager, country_id: str = "CR", 
                       num_years: int = 2, wipe_first: bool = False) -> None:
    """Run simulation with clearer steps."""
    try:
        # 1. Setup
        if wipe_first:
            wipe_trading_tables(db_manager)
        
        country_sim = CountrySimulation(db_manager)
        country_sim.initialize_country_actors(country_id)
        
        # 2. Run simulation years
        for year in range(num_years):
            logger.info(f"Simulating year {year}")
            country_sim.simulate_year(country_id, year)
            
        logger.info("Simulation completed")
        
    except Exception as e:
        logger.error(f"Simulation failed: {e}")
        raise

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Coffee Supply Chain Simulator')
    parser.add_argument('--wipe', action='store_true', 
                       help='Wipe trading tables before simulation')
    return parser.parse_args()

def run_country_simulation(db_config: Dict, country_id: str, year: int):
    """Run simulation for a single country."""
    db_manager = DatabaseManager(db_config)
    country_sim = CountrySimulation(db_manager)
    country_sim.initialize_country_actors(country_id)
    country_sim.simulate_year(country_id, year)
    db_manager.close()

def run_simulation(db_manager: DatabaseManager, config: Dict):
    """Run simulation with parallel processing."""
    # Initialize countries first
    initializer = CountryInitializer(db_manager)
    
    existing_countries = {
        country.id for country in initializer.country_registry.get_all()
    }
    
    for country_id in config['countries']:
        if country_id not in existing_countries:
            logger.info(f"Initializing new country: {country_id}")
            initializer.initialize_country(country_id)
    
    # Run simulation in parallel
    max_workers = min(multiprocessing.cpu_count(), len(config['countries']))
    
    for year in range(config['num_years']):
        logger.info(f"Simulating year {year}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(run_country_simulation, DB_CONFIG, country_id, year)
                for country_id in config['countries']
            ]
            for future in futures:
                future.result()  # Wait for completion and handle any errors

def main():
    """Main entry point for the supply chain simulator."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    args = parse_args()
    
    # Create simulation config from constants
    config = {
        'countries': COUNTRIES,
        'num_years': NUM_YEARS
    }
    
    try:
        db_manager = DatabaseManager(DB_CONFIG)
        db_manager.wipe_database()
        run_simulation(db_manager, config)
        db_manager.close()
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()