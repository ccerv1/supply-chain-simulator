"""
Coffee Supply Chain Simulator
Main execution script with clear initialization patterns
"""

import logging
import argparse
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

from database.manager import DatabaseManager
from simulations.simulate import CountrySimulation
from simulations.initialize import CountryInitializer
from config.config import DB_CONFIG
from config.simulation import COUNTRIES, NUM_YEARS

logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Coffee Supply Chain Simulator')
    parser.add_argument('--wipe-all', action='store_true', 
                       help='Wipe entire database and start fresh')
    parser.add_argument('--wipe-countries', nargs='+',
                       help='Wipe and reinitialize specific countries')
    parser.add_argument('--wipe-trading', action='store_true',
                       help='Wipe only trading data')
    parser.add_argument('--countries', nargs='+',
                       help='Run simulation only for specific countries')
    parser.add_argument('--years', type=int, default=NUM_YEARS,
                       help='Number of years to simulate')
    return parser.parse_args()

def setup_database(db_manager: DatabaseManager, wipe_all: bool = False) -> None:
    """Initialize or verify database structure."""
    try:
        if wipe_all:
            logger.info("Wiping entire database...")
            db_manager.wipe_database()
        
        logger.info("Verifying database structure...")
        db_manager.initialize_database()
        logger.info("Database structure verified.")
        
    except Exception as e:
        logger.error(f"Database setup failed: {str(e)}")
        raise

def setup_countries(db_manager: DatabaseManager, countries_to_wipe: List[str] = None) -> None:
    """Initialize countries, optionally wiping specific ones."""
    try:
        initializer = CountryInitializer(db_manager)
        
        # Get existing countries
        existing_countries = {
            country.id for country in initializer.country_registry.get_all()
        }
        
        # Handle country wipes if specified
        if countries_to_wipe:
            logger.info(f"Wiping specified countries: {', '.join(countries_to_wipe)}")
            for country_id in countries_to_wipe:
                initializer.wipe_country(country_id)
                existing_countries.discard(country_id)
        
        # Initialize missing countries
        countries_to_init = [
            country_id for country_id in COUNTRIES 
            if country_id not in existing_countries
        ]
        
        if countries_to_init:
            logger.info(f"Initializing countries: {', '.join(countries_to_init)}")
            for country_id in countries_to_init:
                initializer.initialize_country(country_id)
        else:
            logger.info("All countries are initialized")
            
    except Exception as e:
        logger.error(f"Country setup failed: {str(e)}")
        raise

def run_country_simulation(db_config: Dict, country_id: str, year: int) -> None:
    """Run simulation for a single country."""
    try:
        # Create new database connection for this process
        db_manager = DatabaseManager(db_config)
        
        # Run simulation
        logger.info(f"Running simulation for {country_id} year {year}")
        country_sim = CountrySimulation(db_manager)
        
        # Initialize country and set relationships
        country_sim.initialize_country_actors(country_id)
        country_sim.set_middleman_geographies(year)
        
        # Run trading simulation
        country_sim.simulate_trading_year(year)
        
        # Cleanup
        db_manager.close()
        
    except Exception as e:
        logger.error(f"Error simulating {country_id} year {year}: {str(e)}")
        raise

def run_trading_simulation(db_manager: DatabaseManager, countries: List[str], 
                         num_years: int, wipe_trading: bool = False) -> None:
    """Run trading simulation for specified countries and years."""
    try:
        if wipe_trading:
            logger.info("Wiping trading data...")
            db_manager.wipe_trading_data()
        
        # Run simulation in parallel
        max_workers = min(multiprocessing.cpu_count(), len(countries))
        
        for year in range(num_years):
            logger.info(f"Simulating year {year}")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for country_id in countries:
                    future = executor.submit(
                        run_country_simulation, 
                        DB_CONFIG, 
                        country_id, 
                        year
                    )
                    futures.append(future)
                
                # Wait for all simulations and handle any errors
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Country simulation failed: {str(e)}")
                        raise
                    
    except Exception as e:
        logger.error(f"Trading simulation failed: {str(e)}")
        raise

def main():
    """Main entry point with clear initialization steps."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    args = parse_args()
    countries_to_simulate = args.countries or COUNTRIES
    
    try:
        db_manager = DatabaseManager(DB_CONFIG)
        
        # 1. Setup database structure
        setup_database(db_manager, args.wipe_all)
        
        # 2. Setup countries
        setup_countries(db_manager, args.wipe_countries)
        
        # 3. Run trading simulation
        run_trading_simulation(
            db_manager, 
            countries_to_simulate,
            args.years,
            args.wipe_trading
        )
        
        db_manager.close()
        logger.info("Simulation completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()