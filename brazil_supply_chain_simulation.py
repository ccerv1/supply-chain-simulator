import numpy as np
import pandas as pd
import multiprocessing as mp
from functools import partial
import time
import matplotlib.pyplot as plt

# Simulation Parameters
NUM_FARMERS = 500_000
NUM_MIDDLEMEN = 1_000
NUM_EXPORTERS = 100
MAX_BUYERS_PER_FARMER = 3
MAX_EXPORTERS_PER_MIDDLEMEN = 3
NUM_SIMULATIONS = 1
COUNTRIES = ['Brazil']  # Add or remove countries as needed

def run_single_simulation(country, num_farmers, num_middlemen, num_exporters):
    # Step 1: Generate farmers
    farmer_ids = np.arange(num_farmers)
    total_production = 2021879291  # Brazil's total production
    production_distribution = np.random.lognormal(
        mean=np.log(total_production / num_farmers) - 0.5,
        sigma=0.8,
        size=num_farmers
    )
    production_distribution = np.clip(production_distribution, 10, total_production * 0.01)
    production_weights = production_distribution / production_distribution.sum() * total_production

    # Step 2: Generate middlemen weights (log-normal distribution)
    middleman_ids = np.arange(num_middlemen)
    min_capacity = max(1000, total_production * 0.0001)
    max_capacity = min_capacity * np.random.uniform(10, 20)
    mean_capacity = total_production / num_middlemen
    middleman_weights = np.random.lognormal(mean=np.log(mean_capacity), sigma=1.0, size=num_middlemen)
    middleman_weights = np.clip(middleman_weights, min_capacity, max_capacity)
    middleman_weights /= middleman_weights.sum()  # Normalize to sum to 1

    # Step 3: Assign farmers to up to 3 middlemen
    buyers_per_farmer = np.random.randint(1, MAX_BUYERS_PER_FARMER + 1, size=num_farmers)
    assigned_middlemen = np.random.choice(
        middleman_ids, size=(num_farmers, MAX_BUYERS_PER_FARMER), replace=True, p=middleman_weights
    )
    # Mask for valid middlemen assignments
    masks = np.arange(MAX_BUYERS_PER_FARMER) < buyers_per_farmer[:, None]
    assigned_middlemen = np.where(masks, assigned_middlemen, -1)

    # Distribute production shares among middlemen
    shares = np.random.dirichlet(np.ones(MAX_BUYERS_PER_FARMER), size=num_farmers)
    shares *= masks  # Zero out shares for invalid assignments

    # Create DataFrame of farmer to middleman assignments
    farmer_to_middleman = pd.DataFrame({
        'farmer_id': np.repeat(farmer_ids, MAX_BUYERS_PER_FARMER),
        'middleman_id': assigned_middlemen.flatten(),
        'share': (shares * production_weights[:, None]).flatten()
    })
    farmer_to_middleman = farmer_to_middleman.query("middleman_id != -1")

    # Step 4: Assign middlemen to up to 3 exporters
    exporters_per_middleman = np.random.randint(1, MAX_EXPORTERS_PER_MIDDLEMEN + 1, size=num_middlemen)
    assigned_exporters = np.random.choice(
        np.arange(num_exporters), size=(num_middlemen, MAX_EXPORTERS_PER_MIDDLEMEN), replace=True
    )
    # Mask for valid exporter assignments
    masks = np.arange(MAX_EXPORTERS_PER_MIDDLEMEN) < exporters_per_middleman[:, None]
    assigned_exporters = np.where(masks, assigned_exporters, -1)

    # Distribute middlemen's total shares among exporters
    middleman_totals = farmer_to_middleman.groupby('middleman_id')['share'].sum().reindex(middleman_ids).values
    exporter_shares = np.random.dirichlet(np.ones(MAX_EXPORTERS_PER_MIDDLEMEN), size=num_middlemen)
    exporter_shares *= masks  # Zero out invalid shares
    exporter_shares = exporter_shares * middleman_totals[:, None]  # Scale by middleman total

    # Create DataFrame of middleman to exporter assignments
    middleman_to_exporter = pd.DataFrame({
        'middleman_id': np.repeat(middleman_ids, MAX_EXPORTERS_PER_MIDDLEMEN),
        'exporter_id': assigned_exporters.flatten(),
        'quantity': exporter_shares.flatten()
    })
    middleman_to_exporter = middleman_to_exporter.query("exporter_id != -1")

    # Step 5: Map farmers to exporters through middlemen
    farmer_to_exporter = farmer_to_middleman.merge(
        middleman_to_exporter, on='middleman_id', how='inner'
    )
    # Adjust the share based on the proportion passed from middleman to exporter
    farmer_to_exporter['final_share'] = farmer_to_exporter['share'] * (
        farmer_to_exporter['quantity'] / middleman_totals[farmer_to_exporter['middleman_id']]
    )

    # Step 6: Calculate unique farmers contributing to each exporter
    unique_farmers_per_exporter = farmer_to_exporter.groupby('exporter_id')['farmer_id'].nunique().reset_index()
    unique_farmers_per_exporter['country'] = country
    # Add simulation ID for tracking
    unique_farmers_per_exporter['simulation_id'] = np.random.randint(1, 1e9)
    
    # Optional: Calculate total volume per exporter
    exporter_volumes = farmer_to_exporter.groupby('exporter_id')['final_share'].sum().reset_index()
    exporter_volumes['country'] = country
    exporter_volumes['simulation_id'] = unique_farmers_per_exporter['simulation_id'].iloc[0]

    # Merge unique farmers and volumes
    exporter_metrics = unique_farmers_per_exporter.merge(exporter_volumes, on=['exporter_id', 'country', 'simulation_id'])
    exporter_metrics.rename(columns={'farmer_id': 'unique_farmers', 'final_share': 'total_volume'}, inplace=True)

    return exporter_metrics

def run_country_simulations(country, num_simulations, num_farmers, num_middlemen, num_exporters):
    results = []
    for _ in range(num_simulations):
        result = run_single_simulation(country, num_farmers, num_middlemen, num_exporters)
        results.append(result)
    return pd.concat(results, ignore_index=True)

def run_all_simulations(countries, num_simulations, num_farmers, num_middlemen, num_exporters):
    # Start timing
    start_time = time.time()

    # Use multiprocessing to parallelize across countries
    with mp.Pool(processes=len(countries)) as pool:
        results = pool.map(
            partial(
                run_country_simulations,
                num_simulations=num_simulations,
                num_farmers=num_farmers,
                num_middlemen=num_middlemen,
                num_exporters=num_exporters,
            ),
            countries,
        )
    # Combine results from all countries
    all_results = pd.concat(results, ignore_index=True)

    # End timing
    end_time = time.time()
    print(f"Total simulation time: {end_time - start_time:.2f} seconds")

    return all_results

if __name__ == "__main__":
    # Run simulations
    simulation_results = run_all_simulations(
        COUNTRIES, NUM_SIMULATIONS, NUM_FARMERS, NUM_MIDDLEMEN, NUM_EXPORTERS
    )

    # Save results to CSV
    simulation_results.to_csv('simulation_results.csv', index=False)
    print("Simulation results saved to 'simulation_results.csv'")

    # Optional: Analyze and plot results for one country
    country_to_analyze = 'Brazil'  # Change to any country you wish to analyze
    country_results = simulation_results[simulation_results['country'] == country_to_analyze]

    # Plot histogram of unique farmers per exporter
    plt.figure(figsize=(10, 6))
    plt.hist(country_results['unique_farmers'], bins=50, alpha=0.7, color='blue')
    plt.xlabel('Number of Unique Farmers')
    plt.ylabel('Frequency')
    plt.title(f'Distribution of Unique Farmers per Exporter in {country_to_analyze}')
    plt.show()

    # Plot histogram of total volume per exporter
    plt.figure(figsize=(10, 6))
    plt.hist(country_results['total_volume'], bins=50, alpha=0.7, color='green')
    plt.xlabel('Total Volume per Exporter')
    plt.ylabel('Frequency')
    plt.title(f'Distribution of Total Volume per Exporter in {country_to_analyze}')
    plt.yscale('log')  # Use log scale for better visualization of long tail
    plt.show()