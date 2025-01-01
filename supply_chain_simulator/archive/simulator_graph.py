from dataclasses import dataclass
import json
import pandas as pd
import numpy as np
from pathlib import Path
import time


BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / 'data'
COUNTRY_CODE_MAPPING = json.load(open(DATA_DIR / 'country_codes.json'))
COUNTRY_ASSUMPTIONS = pd.read_csv(DATA_DIR / 'country_assums.csv')


@dataclass
class SimulationConfig:
    country_name: str = "Default"
    total_production: int = 10_000_000 * 60
    num_farmers: int = 500_000
    num_middlemen: int = 1000
    num_exporters: int = 100
    max_buyers_per_farmer: int = 3
    max_exporters_per_middleman: int = 3
    farmer_production_sigma: float = 0.8
    middleman_capacity_sigma: float = 0.5
    exporter_pareto_alpha: float = 1.16
    traceability_rate: float = 0.5
    eu_export_ratio: float = 0.4  # Ratio of total production going to EU

    @classmethod
    def from_country(cls, country_name: str):
        """Create a config instance with values from country assumptions CSV if available."""
        config = cls(country_name=country_name)
        
        if country_name in COUNTRY_ASSUMPTIONS['country'].values:
            country_data = COUNTRY_ASSUMPTIONS[COUNTRY_ASSUMPTIONS['country'] == country_name].iloc[0]
            
            # Update any matching attributes from the CSV
            for column in country_data.index:
                if hasattr(config, column) and pd.notna(country_data[column]):
                    setattr(config, column, int(country_data[column]))
        
        return config


class Simulation:
    def __init__(self, config: SimulationConfig = None):
        self.config = config or SimulationConfig()
        self.transactions = None
        self.country_name = self.config.country_name
        self.country_code = next((k for k, v in COUNTRY_CODE_MAPPING.items() 
                                if v['full'] == self.country_name), 'xx')
        
    def __str__(self):
        return f"Simulation({self.country_name})"
    
    def __repr__(self):
        return self.__str__()
    
    @staticmethod
    def pad_id(identifier, total):
        """Returns a zero-padded string representation of an ID."""
        return str(identifier).zfill(len(str(total)))
    
    def seed_supply_chain(self):
        """Initialize the supply chain network and generate initial transactions."""
        start_time = time.time()
        
        # Generate node volumes/capacities
        farmer_volumes = self._generate_farmer_volumes()
        middleman_weights = self._generate_middleman_weights()
        exporter_weights = self._generate_exporter_weights()
        
        # Generate transactions through the supply chain
        transactions = []
        
        # 1. Farmer to Middleman connections
        f2m_transactions = self._create_farmer_to_middleman_edges(farmer_volumes)
        transactions.extend(f2m_transactions)
        
        # 2. Middleman to Exporter connections (with traceability)
        m2e_transactions = self._create_middleman_to_exporter_edges(f2m_transactions)
        transactions.extend(m2e_transactions)
        
        # 3. Exporter to Importer (EU/non-EU) connections
        e2i_transactions = self._create_exporter_to_importer_edges(m2e_transactions)
        transactions.extend(e2i_transactions)
        
        self.transactions = pd.DataFrame(transactions)
        return self.transactions

    def _create_farmer_to_middleman_edges(self, farmer_volumes):
        """Create edges between farmers and middlemen."""
        transactions = []
        
        for farmer_id in range(self.config.num_farmers):
            volume = farmer_volumes[farmer_id]
            # Select random number of middlemen (1 to max_buyers_per_farmer)
            num_buyers = np.random.randint(1, self.config.max_buyers_per_farmer + 1)
            middlemen = np.random.choice(
                self.config.num_middlemen, 
                size=num_buyers, 
                replace=False
            )
            # Split volume among selected middlemen
            split_ratios = np.random.dirichlet(np.ones(num_buyers))
            
            for idx, middleman_id in enumerate(middlemen):
                transactions.append({
                    'source': f"{self.country_code.upper()}_FARMER_{self.pad_id(farmer_id, self.config.num_farmers)}",
                    'target': f"{self.country_code.upper()}_MIDDLEMAN_{self.pad_id(middleman_id, self.config.num_middlemen)}",
                    'volume': volume * split_ratios[idx],
                    'farmer_id': farmer_id  # Track origin
                })
        
        return transactions

    def _create_middleman_to_exporter_edges(self, f2m_transactions):
        """Create edges between middlemen and exporters, respecting traceability."""
        transactions = []
        
        # Group by middleman to get their total volumes and farmer sources
        middleman_data = {}
        for t in f2m_transactions:
            mid = t['target']
            if mid not in middleman_data:
                middleman_data[mid] = {'volume': 0, 'farmers': {}}
            middleman_data[mid]['volume'] += t['volume']
            middleman_data[mid]['farmers'][t['farmer_id']] = t['volume']

        for mid, data in middleman_data.items():
            # Determine if this middleman maintains traceability
            maintains_traceability = np.random.random() < self.config.traceability_rate
            
            if maintains_traceability:
                # For each farmer's volume, send to one random exporter
                for farmer_id, volume in data['farmers'].items():
                    exporter_id = np.random.randint(0, self.config.num_exporters)
                    transactions.append({
                        'source': mid,
                        'target': f"{self.country_code.upper()}_EXPORTER_{self.pad_id(exporter_id, self.config.num_exporters)}",
                        'volume': volume,
                        'farmer_id': farmer_id  # Maintain traceability
                    })
            else:
                # Split total volume randomly among exporters
                num_exporters = np.random.randint(1, self.config.max_exporters_per_middleman + 1)
                exporters = np.random.choice(self.config.num_exporters, size=num_exporters, replace=False)
                split_ratios = np.random.dirichlet(np.ones(num_exporters))
                
                for idx, exporter_id in enumerate(exporters):
                    transactions.append({
                        'source': mid,
                        'target': f"{self.country_code.upper()}_EXPORTER_{self.pad_id(exporter_id, self.config.num_exporters)}",
                        'volume': data['volume'] * split_ratios[idx],
                        'farmer_id': None  # Traceability lost
                    })
        
        return transactions

    def _create_exporter_to_importer_edges(self, m2e_transactions):
        """Create edges between exporters and importers (EU/non-EU)."""
        transactions = []
        
        # Group by exporter
        exporter_volumes = {}
        for t in m2e_transactions:
            eid = t['target']
            if eid not in exporter_volumes:
                exporter_volumes[eid] = {'volume': 0, 'traceable_volume': 0, 'farmer_ids': set()}
            exporter_volumes[eid]['volume'] += t['volume']
            if t['farmer_id'] is not None:
                exporter_volumes[eid]['traceable_volume'] += t['volume']
                exporter_volumes[eid]['farmer_ids'].add(t['farmer_id'])

        # For each exporter, split between EU and non-EU
        for eid, data in exporter_volumes.items():
            # Prefer sending traceable coffee to EU
            eu_volume = min(
                data['volume'] * self.config.eu_export_ratio,
                data['traceable_volume']
            )
            non_eu_volume = data['volume'] - eu_volume
            
            if eu_volume > 0:
                transactions.append({
                    'source': eid,
                    'target': f"{self.country_code.upper()}_EU_IMPORTER",
                    'volume': eu_volume,
                    'farmer_ids': list(data['farmer_ids']) if data['farmer_ids'] else None
                })
            
            if non_eu_volume > 0:
                transactions.append({
                    'source': eid,
                    'target': f"{self.country_code.upper()}_NON_EU_IMPORTER",
                    'volume': non_eu_volume,
                    'farmer_ids': None
                })
        
        return transactions

    def _generate_farmer_volumes(self):
        """Generate production volumes for farmers using lognormal distribution."""
        mean_volume = self.config.total_production / self.config.num_farmers
        
        # Using lognormal distribution to generate realistic volumes
        volumes = np.random.lognormal(
            mean=np.log(mean_volume), 
            sigma=self.config.farmer_production_sigma, 
            size=self.config.num_farmers
        )
        
        # Normalize to ensure total production matches config
        volumes = volumes * (self.config.total_production / volumes.sum())
        return volumes

    def _generate_middleman_weights(self):
        """Generate capacity weights for middlemen using lognormal distribution."""
        weights = np.random.lognormal(
            mean=0, 
            sigma=self.config.middleman_capacity_sigma, 
            size=self.config.num_middlemen
        )
        return weights / weights.sum()

    def _generate_exporter_weights(self):
        """Generate capacity weights for exporters using Pareto distribution."""
        weights = np.random.pareto(
            a=self.config.exporter_pareto_alpha, 
            size=self.config.num_exporters
        )
        return weights / weights.sum()


def main():
    config = SimulationConfig.from_country("Colombia")
    sim = Simulation(config)
    
    transactions = sim.seed_supply_chain()
    #new_transactions = sim.simulate_switches()
    eu_exports = sim.simulate_eu_exports()

    #print(transactions)
    #print(new_transactions)
    #print(eu_exports)

if __name__ == "__main__":
    main()
