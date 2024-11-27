# Supply Chain Simulator

The Supply Chain Simulator is a Python-based project designed to model and simulate the supply chain dynamics of agricultural products from farmers to exporters. The project consists of two main scripts: `prep_country_assums.py` and `seed_country_chains.py`.

## Project Structure

- **prep_country_assums.py**: This script processes country-specific assumptions and data, including trade and geographical data, to prepare a dataset (`country_assums.csv`) that includes the number of farmers, total production, and export volumes categorized by EU and other destinations.

- **seed_country_chains.py**: This script simulates the supply chain for each country using the prepared data. It models the production distribution among farmers, the capacity of middlemen, and the capacity of exporters. The results are saved as flow data, detailing the connections and quantities between farmers, middlemen, and exporters.

## How It Works

### Data Preparation (`prep_country_assums.py`)

1. **Load Data**: The script loads trade data from `comtrade_2019.csv` and geographical data from `jebena_geo_map_dataset.csv`.

2. **Process Trade Data**: It processes the trade data to categorize exports as either to the EU or other destinations.

3. **Process Geographical Data**: It processes geographical data to estimate the number of farmers and total production for each country.

4. **Export Data**: The processed data is combined and exported to `country_assums.csv`, which is used by the simulation script.

### Simulation (`seed_country_chains.py`)

1. **Load Simulation Data**: The script loads the prepared data from `country_assums.csv`.

2. **Run Simulations**: It runs simulations for specified countries, modeling the supply chain from farmers to exporters. The simulation includes:
   - Generating farmers and their production capacities.
   - Assigning farmers to middlemen and middlemen to exporters.
   - Calculating the distribution of production and export volumes.

3. **Output Results**: The results of the simulations, including flow data, are saved to `global_flows.parquet`.

## Usage

1. **Prepare Data**: Run `prep_country_assums.py` to generate the `country_assums.csv` file.
   ```bash
   python prep_country_assums.py
   ```

2. **Run Simulations**: Execute `seed_country_chains.py` to perform the simulations and generate results.
   ```bash
   python seed_country_chains.py
   ```

3. **Analyze Results**: Review the `global_flows.parquet` for detailed flow data and use the visualization features to analyze specific countries.

## Requirements

- Python 3.x
- Pandas
- NumPy
- Multiprocessing

Ensure all dependencies are installed before running the scripts.
