"""
Coffee Supply Chain Simulator
Main execution script
"""

import logging
from database.manager import DatabaseManager
from simulations.simulate import CountrySimulation
from config.simulation import COUNTRIES, NUM_YEARS
from config.config import DB_CONFIG

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    db_manager = None
    try:
        # Initialize database first
        db_manager = DatabaseManager(DB_CONFIG)
        db_manager.wipe_database()
        db_manager.initialize_database()
        
        sims = {}
        for country in COUNTRIES:   
            logger.info(f"**** Initializing country: {country} ****")
            country_sim = CountrySimulation(db_manager)
            country_sim.initialize_country_actors(country_id=country)
            sims[country] = country_sim

        logger.info("\n\n**** Simulating years ****\n\n")
        for year in range(0, NUM_YEARS):
            for country in COUNTRIES:
                country_sim = sims[country]
                logger.info(f"**** Simulating Year {year} / Country {country} ****")
                country_sim.set_middleman_geographies(year)
                country_sim.simulate_trading_year(year)
                    
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        return 1
    finally:
        if db_manager:
            db_manager.close()
    
    return 0

if __name__ == "__main__":
    exit(main())