import numpy as np
import json
import pandas as pd
import random
DATA_PATH = 'data/simulation_data.json'

class Farmer:
    def __init__(self, country, production):
        self.country = country
        self.production = production
        self.sold_to_middlemen = 0

class Middleman:
    def __init__(self, country, capacity):
        self.country = country
        self.capacity = capacity
        self.bought_from_farmers = 0
        self.sold_to_exporters = 0

class Exporter:
    def __init__(self, country, capacity, eu_capacity, other_capacity, local_capacity):
        self.country = country
        self.capacity = capacity
        self.eu_capacity = eu_capacity
        self.other_capacity = other_capacity
        self.local_capacity = local_capacity
        self.bought_from_middlemen = 0
        self.exported_eu = 0
        self.exported_other = 0
        self.sold_locally = 0

class SupplyChain:
    def __init__(self, country):
        self.country = country
        self.farmers = []
        self.middlemen = []
        self.exporters = []
        
    def print_details(self):
        summary = self.get_summary_data()
        print(f"\n=== Supply Chain Summary for {self.country} ===\n")
        
        self._print_farmers_summary(summary)
        self._print_middlemen_summary(summary)
        self._print_exporters_summary(summary)

    def _print_farmers_summary(self, summary):
        print(f"FARMERS ({summary['num_farmers']:,d}):")
        print(f"  Total Production: {summary['total_production']:,.0f} kg")
        print(f"  Min Production: {summary['min_farmer_production']:,.0f} kg")
        print(f"  Max Production: {summary['max_farmer_production']:,.0f} kg")
        print(f"  Mean Production: {summary['mean_farmer_production']:,.0f} kg")
        print(f"  Median Production: {summary['median_farmer_production']:,.0f} kg") 
        print(f"  Std Dev: {summary['std_farmer_production']:,.0f} kg")

    def _print_middlemen_summary(self, summary):
        print(f"\nMIDDLEMEN ({summary['num_middlemen']:,d}):")
        print(f"  Total Capacity: {summary['total_middlemen_capacity']:,.0f} kg")
        print(f"  Min Capacity: {summary['min_middleman_capacity']:,.0f} kg")
        print(f"  Max Capacity: {summary['max_middleman_capacity']:,.0f} kg")
        print(f"  Mean Capacity: {summary['mean_middleman_capacity']:,.0f} kg")
        print(f"  Median Capacity: {summary['median_middleman_capacity']:,.0f} kg") 
        print(f"  Std Dev: {summary['std_middleman_capacity']:,.0f} kg")

    def _print_exporters_summary(self, summary):
        print(f"\nEXPORTERS ({summary['num_exporters']:,d}):")
        print(f"  Total Exports: {summary['total_export_capacity']:,.0f} kg")
        print(f"  EU Exports: {summary['eu_exports']:,.0f} kg ({summary['num_eu_exporters']:,d} exporters)")
        print(f"  Other Markets Exports: {summary['other_exports']:,.0f} kg")
        print(f"  Local Market Sales: {summary['local_sales']:,.0f} kg")
        print(f"  Min Capacity: {summary['min_exporter_capacity']:,.0f} kg")
        print(f"  Max Capacity: {summary['max_exporter_capacity']:,.0f} kg")
        print(f"  Mean Capacity: {summary['mean_exporter_capacity']:,.0f} kg")
        print(f"  Median Capacity: {summary['median_exporter_capacity']:,.0f} kg") 
        print(f"  Std Dev: {summary['std_exporter_capacity']:,.0f} kg")

    def get_summary_data(self):
        """Returns a dictionary summarizing the supply chain data."""
        farmer_productions = [f.production for f in self.farmers]
        middleman_capacities = [m.capacity for m in self.middlemen]
        exporter_capacities = [e.capacity for e in self.exporters]

        return {
            'country': self.country,
            'num_farmers': len(self.farmers),
            'total_production': sum(farmer_productions),
            'min_farmer_production': min(farmer_productions),
            'max_farmer_production': max(farmer_productions),
            'mean_farmer_production': np.mean(farmer_productions),
            'median_farmer_production': np.median(farmer_productions),
            'std_farmer_production': np.std(farmer_productions),
            'num_middlemen': len(self.middlemen),
            'total_middlemen_capacity': sum(middleman_capacities),
            'min_middleman_capacity': min(middleman_capacities),
            'max_middleman_capacity': max(middleman_capacities),
            'mean_middleman_capacity': np.mean(middleman_capacities),
            'median_middleman_capacity': np.median(middleman_capacities),
            'std_middleman_capacity': np.std(middleman_capacities),
            'num_exporters': len(self.exporters),
            'total_export_capacity': sum(exporter_capacities),
            'eu_exports': sum(e.eu_capacity for e in self.exporters),
            'other_exports': sum(e.other_capacity for e in self.exporters),
            'local_sales': sum(e.local_capacity for e in self.exporters),
            'num_eu_exporters': sum(1 for e in self.exporters if e.eu_capacity > 0),
            'min_exporter_capacity': min(exporter_capacities),
            'max_exporter_capacity': max(exporter_capacities),
            'mean_exporter_capacity': np.mean(exporter_capacities),
            'median_exporter_capacity': np.median(exporter_capacities),
            'std_exporter_capacity': np.std(exporter_capacities)
        }

    def get_summary_dataframe(self):
        """Returns a pandas DataFrame summarizing the supply chain data."""
        return pd.DataFrame([self.get_summary_data()])

    def run_simulation(self):
        self._exporters_select_middlemen()
        self._middlemen_select_farmers()

    def _exporters_select_middlemen(self):
        for exporter in self.exporters:
            available_middlemen = [m for m in self.middlemen if m.sold_to_exporters < 5]
            selected_middlemen = random.sample(available_middlemen, min(len(available_middlemen), 5))
            for middleman in selected_middlemen:
                if exporter.bought_from_middlemen < exporter.capacity:
                    amount_to_buy = min(middleman.capacity - middleman.bought_from_farmers, exporter.capacity - exporter.bought_from_middlemen)
                    middleman.sold_to_exporters += amount_to_buy
                    exporter.bought_from_middlemen += amount_to_buy

    def _middlemen_select_farmers(self):
        for middleman in self.middlemen:
            available_farmers = [f for f in self.farmers if f.sold_to_middlemen < 3]
            selected_farmers = random.sample(available_farmers, min(len(available_farmers), 3))
            for farmer in selected_farmers:
                if middleman.bought_from_farmers < middleman.capacity:
                    amount_to_buy = min(farmer.production - farmer.sold_to_middlemen, middleman.capacity - middleman.bought_from_farmers)
                    farmer.sold_to_middlemen += amount_to_buy
                    middleman.bought_from_farmers += amount_to_buy

class SupplyChainGenerator:
    def __init__(self, data_path=DATA_PATH):
        with open(data_path) as f:
            self.data = json.load(f)
            
    def generate_supply_chain(self, country):
        """Generate a complete supply chain for a given country"""
        supply_chain = SupplyChain(country)
        supply_chain.farmers = self._create_farmers(country)
        supply_chain.middlemen = self._create_middlemen(country)
        supply_chain.exporters = self._create_exporters(country)
        return supply_chain
    
    def _create_farmers(self, country):
        country_data = self.data['production'].get(country)
        if country_data is None:
            raise ValueError(f"No production data found for country: {country}")
            
        num_farmers = country_data['num_farmers']
        total_production = country_data['est_production']
        
        production_distribution = self._generate_production_distribution(num_farmers, total_production)
        
        return [Farmer(country, prod) for prod in production_distribution]

    def _generate_production_distribution(self, num_farmers, total_production):
        production_distribution = np.random.lognormal(
            mean=np.log(total_production / num_farmers) - 0.5,
            sigma=0.8,
            size=num_farmers
        )
        
        min_production = 10  # Minimum 10 kg per farmer
        max_production = total_production * 0.01  # Maximum 1% of total production
        production_distribution = np.clip(production_distribution, min_production, max_production)
        
        production_distribution = production_distribution / production_distribution.sum() * total_production
        return production_distribution

    def _create_middlemen(self, country):
        country_data = self.data['production'].get(country)
        if country_data is None:
            raise ValueError(f"No production data found for country: {country}")
        
        total_production = country_data['est_production']
        num_middlemen = int(np.sqrt(country_data['num_farmers'])) * 10
        
        capacities = self._generate_middlemen_capacities(num_middlemen, total_production)
        
        return [Middleman(country, cap) for cap in capacities]

    def _generate_middlemen_capacities(self, num_middlemen, total_production):
        min_capacity = max(1000, total_production * 0.0001)
        max_capacity = min_capacity * np.random.uniform(10, 20)
        
        mean_capacity = total_production / num_middlemen
        sigma = 1.0
        
        capacities = np.random.lognormal(mean=np.log(mean_capacity), sigma=sigma, size=num_middlemen)
        capacities = np.clip(capacities, min_capacity, max_capacity)
        
        capacities = capacities / capacities.sum() * total_production
        return capacities

    def _create_exporters(self, country):
        country_exports = self.data['exports'].get(country)
        if country_exports is None:
            raise ValueError(f"No export data found for country: {country}")
        
        country_data = self.data['production'].get(country)
        total_production = country_data['est_production']
        eu_exports = country_exports.get('EU', 0)
        other_exports = country_exports.get('Other', 0)
        total_exports = eu_exports + other_exports
        local_sales = total_production - total_exports 
        
        num_middlemen = len(self._create_middlemen(country))
        num_exporters = int(np.sqrt(num_middlemen))
        
        capacities = self._generate_exporter_capacities(num_exporters, total_production)
        
        return self._distribute_exports(capacities, country, eu_exports, other_exports, local_sales)

    def _generate_exporter_capacities(self, num_exporters, total_production):
        min_capacity = max(20000, total_production * 0.001)
        max_capacity = total_production * 0.4
        
        alpha = 1.16
        capacities = np.random.pareto(alpha, num_exporters)
        capacities = capacities / capacities.sum() * total_production
        
        clipped_volume = 0
        for i in range(len(capacities)):
            if capacities[i] < min_capacity:
                clipped_volume += capacities[i] - min_capacity
                capacities[i] = min_capacity
            elif capacities[i] > max_capacity:
                clipped_volume += capacities[i] - max_capacity
                capacities[i] = max_capacity
        
        if clipped_volume != 0:
            non_maxed_indices = capacities < max_capacity
            if np.any(non_maxed_indices):
                distribution_weights = capacities[non_maxed_indices]
                distribution_weights = distribution_weights / distribution_weights.sum()
                capacities[non_maxed_indices] += clipped_volume * distribution_weights
        
        return capacities

    def _distribute_exports(self, capacities, country, eu_exports, other_exports, local_sales):
        eu_capable_capacities = []
        other_only_capacities = []
        
        for cap in capacities:
            if cap >= 100000 and np.random.random() < 0.7:
                eu_capable_capacities.append(cap)
            else:
                other_only_capacities.append(cap)
        
        eu_capable_capacities = self._normalize_capacities(eu_capable_capacities, eu_exports)
        other_only_capacities = self._normalize_capacities(other_only_capacities, other_exports)
        
        exporters = [Exporter(country, cap, cap, 0, 0) for cap in eu_capable_capacities]
        exporters.extend(Exporter(country, cap, 0, cap, 0) for cap in other_only_capacities)
        
        local_capacities = np.random.lognormal(mean=0, sigma=1, size=len(exporters))
        local_capacities = local_capacities / local_capacities.sum() * local_sales
        
        for exporter, local_cap in zip(exporters, local_capacities):
            exporter.local_capacity = local_cap
        
        return exporters

    def _normalize_capacities(self, capacities, total):
        if capacities:
            capacities = np.array(capacities)
            capacities = capacities / capacities.sum() * total
        return capacities

class WorldSimulation:
    def __init__(self, data_path=DATA_PATH):
        self.generator = SupplyChainGenerator(data_path)
        self.supply_chains = {}

    def generate_all_supply_chains(self):
        countries = self.generator.data['production'].keys()
        for country in countries:
            if country == 'Other':
                continue
            try:
                supply_chain = self.generator.generate_supply_chain(country)
                self.supply_chains[country] = supply_chain
            except ValueError as e:
                print(f"Error generating supply chain for {country}: {e}")

    def print_all_details(self):
        for country, supply_chain in self.supply_chains.items():
            supply_chain.print_details()

    def generate_summary_dataframe(self):
        summary_data = []
        for country, supply_chain in self.supply_chains.items():
            summary_data.append(supply_chain.get_summary_data())
        return pd.DataFrame(summary_data)

# Usage example:
world_simulation = WorldSimulation()
world_simulation.generate_all_supply_chains()
for supply_chain in world_simulation.supply_chains.values():
    supply_chain.run_simulation()
df = world_simulation.generate_summary_dataframe()
df.to_csv('data/supply_chain_summary.csv', index=False)