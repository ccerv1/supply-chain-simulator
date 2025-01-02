from pathlib import Path

# Directory settings
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'data'

# Database settings
DB_PATH = DATA_DIR / 'supply_chain.db'

# Simulation constants
FARMER_PLOT_THRESHOLDS = {
    'low': {
        'threshold': 0.2,  # Bottom 20%
        'plots': 1
    },
    'high': {
        'threshold': 0.8,  # Top 20%
        'plots': 3
    }
}

LOGNORMAL_ADJUSTMENT = 0.5

# Data file paths
COUNTRY_CODES_PATH = DATA_DIR / 'country_codes.json'
COUNTRY_ASSUMPTIONS_PATH = DATA_DIR / 'country_assums.csv'
GEOGRAPHY_DATA_PATH = DATA_DIR / '_local/jebena_geodata.csv'