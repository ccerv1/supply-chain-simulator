"""
Coffee Supply Chain Simulator
Main execution script
"""

import logging
from pathlib import Path
from datetime import datetime

from database.manager import DatabaseManager
from simulations.simulate import CountrySimulation

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
DB_PATH = DATA_DIR / 'supply_chain.db'


def main():
    """Main execution function."""
    try:
        # Wipe database before starting
        db = DatabaseManager(DB_PATH)
        logger.info("Wiping database...")
        db.wipe_database()
        logger.info("Database wiped successfully")

        # Initialize country
        country_sim = CountrySimulation(DB_PATH)
        country_sim.initialize_country_actors(country_id="CR")

        for year in range(0, 3):
            country_sim.set_middleman_geographies(year)
            country_sim.simulate_trading_year(year)
            
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())