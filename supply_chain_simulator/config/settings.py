from pathlib import Path

# Directory settings
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / 'data'

# Database settings
DB_PATH = DATA_DIR / 'supply_chain.db'

# Data file paths
COUNTRY_CODES_PATH = DATA_DIR / 'country_codes.json'
COUNTRY_ASSUMPTIONS_PATH = DATA_DIR / 'country_assums.csv'
GEOGRAPHY_DATA_PATH = DATA_DIR / '_local/jebena_geodata.csv'