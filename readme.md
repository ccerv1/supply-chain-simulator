# Supply Chain Simulator

A Python-based simulation of the global coffee supply chain, modeling relationships and trade flows between farmers, middlemen, and exporters.

## Installation & Setup

### 1. PostgreSQL Setup

1. Install PostgreSQL:
```bash
# On macOS with Homebrew:
brew install postgresql@15
brew services start postgresql@15
```

2. Create a database and user:
```bash
# Connect to PostgreSQL
psql postgres

# In the PostgreSQL prompt:
CREATE DATABASE supply_chain_simulator;
CREATE USER postgres WITH PASSWORD 'postgres';  # If not already exists
GRANT ALL PRIVILEGES ON DATABASE supply_chain_simulator TO postgres;
```

3. Configure environment variables:
```bash
# Create .env file in project root
cat > .env << EOL
DB_NAME=supply_chain_simulator
DB_USER=postgres
DB_PASSWORD=postgres
DB_HOST=localhost
DB_PORT=5432
EOL
```

The database uses the following schema structure:
- Countries: Base table with country-level parameters
- Geographies: Regions within countries with production data
- Actors (partitioned by country):
  - Farmers: Production amounts and loyalty scores
  - Middlemen: Competitiveness and loyalty metrics
  - Exporters: EU preferences and competitiveness scores
- Relationships (temporal tracking):
  - Farmer-Middleman relationships
  - Middleman-Exporter relationships
  - Middleman-Geography assignments
- Trade Flows: Actual transactions with volumes and EU designation

### 2. Project Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/supply-chain-simulator.git
cd supply-chain-simulator
```

2. Install Poetry (if not already installed):
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

3. Install dependencies and create virtual environment:
```bash
poetry install
```

4. Activate the virtual environment:
```bash
poetry shell
```

## Running the Simulation

1. Initialize the database:
```bash
python supply_chain_simulator/main.py --wipe-all
```

2. Run simulation for specific countries:
```bash
python supply_chain_simulator/main.py --countries CR UG  # For Costa Rica and Uganda
```

3. Run for all countries with specific years:
```bash
python supply_chain_simulator/main.py --years 5  # Simulate 5 years
```

## Required Data Files

The simulation requires two primary data sources:

1. **Geography Data** (`data/jebena_geo_map_dataset.csv`):
   - Geographic regions within each country
   - Estimated farmer populations
   - Production volumes for arabica and robusta
   - Primary crop types

2. **Country Assumptions** (`data/country_assums.csv`):
   - Country-level parameters
   - Number of middlemen and exporters
   - EU export volumes
   - Relationship parameters (loyalty rates, switching probabilities)

## Simulation Process

### 1. Database Initialization
- Creates tables for actors (farmers, middlemen, exporters)
- Sets up relationship tables (farmer-middleman, middleman-exporter)
- Establishes geography mappings
- Implements partitioning by country

### 2. Country Initialization
- Creates actors based on geography data:
  - Farmers: Distributed across geographies with production volumes
  - Middlemen: Assigned to geographic regions
  - Exporters: Created with EU preferences and competitiveness scores

### 3. Trading Network Creation
For each simulation year:

1. **Geographic Assignment**
   - Middlemen are assigned to geographic regions
   - Assignments can change yearly based on configured rates

2. **Relationship Formation**
   - Initial Year (0):
     - Farmers randomly connect to 1-3 middlemen in their region
     - Middlemen connect to 1-3 exporters
   - Subsequent Years:
     - Relationships update based on loyalty scores
     - New relationships form when old ones end

3. **Trade Flow Generation**
   - Production volumes split among relationships
   - EU sales determined by:
     - Country's total EU export target
     - Exporter's EU preference
     - Random variation

### Randomization Assumptions

1. **Actor Creation**
   - Farmer production: Log-normal distribution around regional averages
   - Middleman capacity: Based on regional farmer counts
   - Exporter capacity: Pareto distribution for size variation

2. **Relationship Formation**
   - Initial connections: Random selection within constraints
   - Relationship changes: Probability based on loyalty scores
   - Geographic changes: Annual probability (default 10%)

3. **Volume Distribution**
   - Dirichlet distribution for fair splitting among relationships
   - EU sales probability weighted by exporter preference

### Key Parameters

- `max_buyers_per_farmer`: Maximum middlemen per farmer (default: 3)
- `max_exporters_per_middleman`: Maximum exporters per middleman (default: 5)
- `farmer_switch_rate`: Annual rate of farmer relationship changes
- `geography_change_rate`: Annual rate of middleman location changes
- `traceability_rate`: Percentage of traceable volumes

## Output Data

The simulation generates:
- Relationship records with temporal validity
- Trade flows with volumes and EU/non-EU designation
- Geographic coverage metrics
- Actor relationship statistics

## Analysis Tools

The simulation includes tools for:
- Network analysis of trading relationships
- Volume distribution analysis
- Geographic coverage assessment
- EU export compliance verification
