from dataclasses import dataclass
from typing import Tuple, List, Dict, Any
import numpy as np
import pandas as pd
import multiprocessing as mp
import time
from pathlib import Path


BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
SIMULATION_DATA_PATH = DATA_DIR / 'simulation_data.csv'
RESULTS_DIR = DATA_DIR / '_local/results'
RESULTS_DIR.mkdir(exist_ok=True)

@dataclass
class SimulationConfig:
    num_middlemen: int = 1000
    num_exporters: int = 100
    max_buyers_per_farmer: int = 3
    max_exporters_per_middleman: int = 3
    farmer_production_sigma: float = 0.8
    middleman_capacity_sigma: float = 1.0
    exporter_pareto_alpha: float = 1.16
    min_farmer_production: float = 10
    max_farmer_production_pct: float = 0.01
    min_middleman_capacity_pct: float = 0.00005
    max_middleman_capacity_pct: float = 0.0005
    min_quantity_to_middleman: float = 10
    min_quantity_to_exporter: float = 1000


SIM_DATA = pd.read_csv(SIMULATION_DATA_PATH)

def get_country_data(country: str, config: SimulationConfig) -> Dict[str, Any]:
    if country not in SIM_DATA['Country'].values:
        raise ValueError(f"Country {country} not found in simulation data")
    country_data = SIM_DATA[SIM_DATA['Country'] == country].iloc[0].to_dict()
    country_data.setdefault('Total Middlemen', config.num_middlemen)
    country_data.setdefault('Total Exporters', config.num_exporters)
    return country_data

def model_farmer_production(country_data: Dict, config: SimulationConfig) -> np.ndarray:
    num_farmers = int(country_data['Total Farmers'])
    total_production = country_data['Total Production']

    production_distribution = np.random.lognormal(
        mean=np.log(total_production / num_farmers) - 0.5,
        sigma=config.farmer_production_sigma,
        size=num_farmers
    )
    return np.clip(
        production_distribution,
        config.min_farmer_production,
        total_production * config.max_farmer_production_pct
    )

def model_middleman_capacity(country_data: Dict, config: SimulationConfig) -> np.ndarray:
    total_production = country_data['Total Production']
    num_middlemen = int(country_data['Total Middlemen'])

    min_capacity = max(1000, total_production * config.min_middleman_capacity_pct)
    max_capacity = total_production * config.max_middleman_capacity_pct * np.random.uniform(10, 20)
    mean_capacity = total_production / num_middlemen

    middleman_weights = np.random.lognormal(
        mean=np.log(mean_capacity),
        sigma=config.middleman_capacity_sigma,
        size=num_middlemen
    )
    return np.clip(
        middleman_weights,
        min_capacity,
        max_capacity
    )

def model_exporter_capacity(country_data: Dict, config: SimulationConfig) -> np.ndarray:
    num_exporters = int(country_data['Total Exporters'])
    exporter_weights = (np.random.pareto(config.exporter_pareto_alpha, num_exporters) + 1)
    return exporter_weights / exporter_weights.sum()

def generate_num_connections(num_entities, connection_probs):
    num_conns, probs = zip(*connection_probs)
    return np.random.choice(num_conns, size=num_entities, p=probs)

def assign_entities(
    source_ids: np.ndarray,
    target_ids: np.ndarray,
    target_weights: np.ndarray,
    max_connections: int,
    connection_probs: List[Tuple[int, float]],
    source_totals: np.ndarray,
    min_value: float = 0
) -> pd.DataFrame:
    
    num_sources = len(source_ids)
    num_connections = generate_num_connections(num_sources, connection_probs)
    num_connections = np.clip(num_connections, 1, max_connections)

    assigned_targets = np.random.choice(
        target_ids,
        size=(num_sources, max_connections),
        replace=True,
        p=target_weights
    )

    masks = np.arange(max_connections) < num_connections[:, None]
    assigned_targets = np.where(masks, assigned_targets, -1)

    shares = np.random.dirichlet(np.ones(max_connections), size=num_sources)
    shares *= masks
    shares = shares * source_totals[:, None]

    assignments = pd.DataFrame({
        'source_id': np.repeat(source_ids, max_connections),
        'target_id': assigned_targets.flatten(),
        'value': shares.flatten()
    })

    valid_mask = assignments['target_id'] != -1
    valid_assignments = assignments[valid_mask]
    total_assigned = valid_assignments.groupby('source_id')['value'].transform('sum')
    min_value_condition = valid_assignments.groupby('source_id')['target_id'].transform('size') * min_value
    valid_assignments.loc[:, 'value'] = np.where(
        min_value_condition <= source_totals[valid_assignments['source_id'].values],
        np.maximum(valid_assignments['value'], min_value),
        source_totals[valid_assignments['source_id'].values] / valid_assignments.groupby('source_id')['target_id'].transform('size')
    )
    assignments.loc[valid_mask, 'value'] = valid_assignments['value']

    return assignments.query("target_id != -1")

def assign_farmers_to_middlemen(
    farmer_ids: np.ndarray,
    middleman_ids: np.ndarray,
    middleman_weights: np.ndarray,
    production_values: np.ndarray,
    config: SimulationConfig,
) -> pd.DataFrame:
    
    connection_probs = [(3, 0.6), (2, 0.3), (1, 0.1)]
    assignments = assign_entities(
        source_ids=farmer_ids,
        target_ids=middleman_ids,
        target_weights=middleman_weights,
        max_connections=config.max_buyers_per_farmer,
        connection_probs=connection_probs,
        source_totals=production_values,
        min_value=config.min_quantity_to_middleman
    )
    assignments.rename(columns={'source_id': 'farmer_id', 'target_id': 'middleman_id'}, inplace=True)
    return assignments

def assign_middlemen_to_exporters(
    middleman_ids: np.ndarray,
    exporter_ids: np.ndarray,
    exporter_weights: np.ndarray,
    middleman_totals: np.ndarray,
    config: SimulationConfig
) -> pd.DataFrame:
    
    connection_probs = [(3, 0.6), (2, 0.3), (1, 0.1)]
    assignments = assign_entities(
        source_ids=middleman_ids,
        target_ids=exporter_ids,
        target_weights=exporter_weights,
        max_connections=config.max_exporters_per_middleman,
        connection_probs=connection_probs,
        source_totals=middleman_totals,
        min_value=config.min_quantity_to_exporter
    )
    assignments.rename(columns={'source_id': 'middleman_id', 'target_id': 'exporter_id'}, inplace=True)
    return assignments


def simulate_country_supply_chain(
    country: str,
    config: SimulationConfig
) -> pd.DataFrame:
    
    country_data = get_country_data(country, config)

    production_distribution = model_farmer_production(country_data, config)
    farmer_ids = np.arange(len(production_distribution))

    middleman_distribution = model_middleman_capacity(country_data, config)
    middleman_weights = middleman_distribution / middleman_distribution.sum()
    middleman_ids = np.arange(len(middleman_weights))

    exporter_weights = model_exporter_capacity(country_data, config)
    exporter_ids = np.arange(len(exporter_weights))

    farmer_to_middleman = assign_farmers_to_middlemen(
        farmer_ids,
        middleman_ids,
        middleman_weights,
        production_distribution,
        config
    )

    middleman_totals_series = farmer_to_middleman.groupby('middleman_id')['value'].sum()
    middleman_totals = middleman_totals_series.reindex(middleman_ids).fillna(0).values

    middleman_to_exporter = assign_middlemen_to_exporters(
        middleman_ids,
        exporter_ids,
        exporter_weights,
        middleman_totals,
        config
    )

    farmer_flows = farmer_to_middleman.copy()
    farmer_flows['source'] = 'F' + farmer_flows['farmer_id'].astype(str)
    farmer_flows['target'] = 'M' + farmer_flows['middleman_id'].astype(str)
    farmer_flows['value'] = farmer_flows['value']
    farmer_flows = farmer_flows[['source', 'target', 'value']]

    exporter_flows = middleman_to_exporter.copy()
    exporter_flows['source'] = 'M' + exporter_flows['middleman_id'].astype(str)
    exporter_flows['target'] = 'E' + exporter_flows['exporter_id'].astype(str)
    exporter_flows['value'] = exporter_flows['value']
    exporter_flows = exporter_flows[['source', 'target', 'value']]

    all_flows = pd.concat([farmer_flows, exporter_flows], ignore_index=True)
    all_flows['country'] = country

    farmer_total = farmer_flows['value'].sum()
    if farmer_total > 0:
        scaling_factor = country_data['Total Production'] / farmer_total
        mask = all_flows['source'].str.startswith('F')
        all_flows.loc[mask, 'value'] = (all_flows.loc[mask, 'value'] * scaling_factor).round().astype(int)

    exporter_total = exporter_flows['value'].sum()
    if exporter_total > 0:  
        scaling_factor = country_data['Total Production'] / exporter_total
        mask = all_flows['source'].str.startswith('M')
        all_flows.loc[mask, 'value'] = (all_flows.loc[mask, 'value'] * scaling_factor).round().astype(int)

    return all_flows


def seed_country_supply_chain(country: str, config: SimulationConfig) -> pd.DataFrame:
    all_flows_list = []
    flows  = simulate_country_supply_chain(country=country, config=config)
    all_flows_list.append(flows)
    return pd.concat(all_flows_list, ignore_index=True)


def seed_all_countries(countries: List[str], config: SimulationConfig) -> pd.DataFrame:
    start_time = time.time()
    with mp.Pool() as pool:
        results = pool.starmap(
            seed_country_supply_chain,
            [(country, config) for country in countries]
        )
    all_flows = pd.concat(results, ignore_index=True)
    end_time = time.time()
    print(f"Total simulation time: {end_time - start_time:.2f} seconds")
    return all_flows

if __name__ == "__main__":
    config = SimulationConfig()
    countries = SIM_DATA['Country'].unique().tolist()
    countries = ['Rwanda', 'Colombia', 'Brazil']
    flows_results = seed_all_countries(countries=countries, config=config)
    
    output_path = RESULTS_DIR / "global_flows.parquet"
    flows_results.to_parquet(output_path, index=False)
    print(f"All simulation flows saved to '{output_path}'")