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
OUTPUT_DIR = BASE_DIR / 'output' / datetime.now().strftime('%Y%m%d_%H%M%S')
DB_PATH = DATA_DIR / 'supply_chain.db'

def setup_directories():
    """Create necessary directories."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def main():
    """Main execution function."""
    try:
        setup_directories()
        
        # Wipe database before starting
        db = DatabaseManager(DB_PATH)
        logger.info("Wiping database...")
        db.wipe_database()
        logger.info("Database wiped successfully")

        # Initialize country
        setup = CountrySimulation(DB_PATH)
        country = setup.initialize_country_actors("CR")
        
        # Run simulation for 2 years
        # simulator = YearlySimulator(DB_PATH)
        # for year in range(1, 3):  # Simulate years 1 and 2
        #     simulator.simulate_year("CR", year)
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())