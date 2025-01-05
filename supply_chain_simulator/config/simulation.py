# Simulation constants
LOGNORMAL_ADJUSTMENT = 0.5
MAX_BUYERS_PER_FARMER: int = 3
MIN_MIDDLEMEN_PER_GEOGRAPHY: int = 2
MIN_MIDDLEMEN_PER_PRODUCING_AREA: int = 4
DEFAULT_RANDOM_SEED: int = 24
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
COUNTRIES = [
    # 'BR',
    # 'CO',
    # 'PE',
    # 'VN',
    # 'ID',    
    # 'ET',
    # 'RW',
    # 'UG',    
    # 'HN',
    # 'GT',
    'CR'
]
NUM_YEARS = 3
