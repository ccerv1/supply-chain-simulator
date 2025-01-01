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
    farmer_switch_rate: float = 0.20
    middleman_switch_rate: float = 0.30
    exports_to_eu: int = 4_000_000
    traceability_rate: float = 0.5


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
        
        # Farmer Volumes
        farmer_volumes = np.random.lognormal(
            mean=np.log(self.config.total_production / self.config.num_farmers) - 0.5,
            sigma=self.config.farmer_production_sigma,
            size=self.config.num_farmers
        )
        farmer_volumes = farmer_volumes / farmer_volumes.sum() * self.config.total_production

        # Middleman and Exporter Weights
        middleman_weights = np.random.lognormal(
            mean=np.log(self.config.total_production / self.config.num_middlemen) - 0.5 * self.config.middleman_capacity_sigma**2,
            sigma=self.config.middleman_capacity_sigma,
            size=self.config.num_middlemen
        )
        middleman_weights /= middleman_weights.sum()

        exporter_weights = (1 + np.random.pareto(self.config.exporter_pareto_alpha, self.config.num_exporters))
        exporter_weights /= exporter_weights.sum()

        # Generate assignments and transactions
        self.transactions = self._generate_transactions(farmer_volumes, middleman_weights, exporter_weights)
        
        total_time = time.time() - start_time
        print(f"Total simulation time: {total_time:.2f}s")
        
        return self.transactions
    
    def _generate_transactions(self, farmer_volumes, middleman_weights, exporter_weights):
        """Helper method to generate transaction network."""
        # Farmer-to-Middleman Assignments
        farmer_matrix = np.random.choice(
            np.arange(self.config.num_middlemen),
            size=(self.config.num_farmers, self.config.max_buyers_per_farmer),
            p=middleman_weights
        )
        
        # Middleman-to-Exporter Assignments
        middleman_matrix = np.random.choice(
            np.arange(self.config.num_exporters),
            size=(self.config.num_middlemen, self.config.max_exporters_per_middleman),
            p=exporter_weights
        )

        # Generate transaction DataFrames
        f2m = self._generate_farmer_to_middleman(farmer_matrix, farmer_volumes, middleman_weights)
        m2e = self._generate_middleman_to_exporter(middleman_matrix, f2m, exporter_weights)
        
        return pd.concat([f2m, m2e], ignore_index=True)
    
    def _generate_farmer_to_middleman(self, farmer_matrix, farmer_volumes, middleman_weights):
        """Generate farmer-to-middleman transactions."""
        farmer_to_middleman_weights = middleman_weights[farmer_matrix]
        farmer_to_middleman_weights /= farmer_to_middleman_weights.sum(axis=1, keepdims=True)
        farmer_to_middleman_volumes = farmer_to_middleman_weights * farmer_volumes[:, None]

        return pd.DataFrame({
            'source': [f"{self.country_code.upper()}_FARMER_{self.pad_id(fid, self.config.num_farmers)}" 
                      for fid in np.repeat(np.arange(self.config.num_farmers), self.config.max_buyers_per_farmer)],
            'target': [f"{self.country_code.upper()}_MIDDLEMAN_{self.pad_id(mid, self.config.num_middlemen)}" 
                      for mid in farmer_matrix.flatten()],
            'volume': farmer_to_middleman_volumes.flatten()
        })
    
    def _generate_middleman_to_exporter(self, middleman_matrix, farmer_to_middleman, exporter_weights):
        """Generate middleman-to-exporter transactions."""
        middleman_to_exporter_weights = exporter_weights[middleman_matrix]
        middleman_to_exporter_weights /= middleman_to_exporter_weights.sum(axis=1, keepdims=True)
        middleman_volumes = farmer_to_middleman.groupby('target')['volume'].sum().values
        middleman_to_exporter_volumes = middleman_to_exporter_weights * middleman_volumes[:, None]

        return pd.DataFrame({
            'source': [f"{self.country_code.upper()}_MIDDLEMAN_{self.pad_id(mid, self.config.num_middlemen)}" 
                      for mid in np.repeat(np.arange(self.config.num_middlemen), self.config.max_exporters_per_middleman)],
            'target': [f"{self.country_code.upper()}_EXPORTER_{self.pad_id(exp, self.config.num_exporters)}" 
                      for exp in middleman_matrix.flatten()],
            'volume': middleman_to_exporter_volumes.flatten()
        })
    
    def simulate_switches(self):
        """Simulate trading partner switches."""
        if self.transactions is None:
            raise ValueError("Must seed supply chain before simulating switches")
            
        start_time = time.time()
        
        # Farmer-to-Middleman switches
        f2m = self.transactions[self.transactions['source'].str.contains('FARMER')]
        switched_f2m = self._switch_targets(
            f2m.copy(),
            'target',
            self.config.farmer_switch_rate,
            [f"{self.country_code.upper()}_MIDDLEMAN_{self.pad_id(mid, self.config.num_middlemen)}" 
             for mid in range(self.config.num_middlemen)]
        )

        # Middleman-to-Exporter switches
        m2e = self.transactions[self.transactions['source'].str.contains('MIDDLEMAN')]
        switched_m2e = self._switch_targets(
            m2e.copy(),
            'target',
            self.config.middleman_switch_rate,
            [f"{self.country_code.upper()}_EXPORTER_{self.pad_id(exp, self.config.num_exporters)}" 
             for exp in range(self.config.num_exporters)]
        )
        
        total_time = time.time() - start_time
        print(f"Total switching time: {total_time:.2f}s")
        
        self.transactions = pd.concat([switched_f2m, switched_m2e]).sort_index()
        return self.transactions
    
    @staticmethod
    def _switch_targets(df, target_col, switch_rate, new_targets):
        """Helper method to switch trading partners."""
        num_switches = int(len(df) * switch_rate)
        switch_indices = np.random.choice(df.index, num_switches, replace=False)
        df.loc[switch_indices, target_col] = np.random.choice(new_targets, size=num_switches)
        return df
    
    def simulate_eu_exports(self):
        """Simulate exports to the EU based on exporter volumes and traceability requirements."""

        start_time = time.time()

        if self.transactions is None:
            raise ValueError("Must seed supply chain before simulating EU exports")

        #
        # 1. Compute total volume per exporter (vectorized)
        #
        exporter_mask = self.transactions['target'].str.contains('EXPORTER')
        exporter_volumes = (
            self.transactions[exporter_mask]
            .groupby('target')['volume']
            .sum()
            .astype(float)
        )

        #
        # 2. Vectorized EU export allocation
        #
        remaining_demand = float(self.config.exports_to_eu)
        eu_shipment_size = 40_000.0

        # Number of full shipments possible for each exporter
        max_shipments = np.floor(exporter_volumes / eu_shipment_size).astype(int)
        total_full_shipments = max_shipments.sum()

        # Allocate full shipments
        eu_export_volumes = pd.Series(0.0, index=exporter_volumes.index)

        if total_full_shipments > 0:
            # Probabilities based on exporter volumes
            probs = exporter_volumes / exporter_volumes.sum()
            # Multinomial draws how many shipments each exporter sends
            selected_counts = np.random.multinomial(
                min(total_full_shipments, int(remaining_demand / eu_shipment_size)),
                probs
            )
            # Assign volumes based on number of shipments * 40k
            eu_export_volumes = pd.Series(selected_counts * eu_shipment_size, index=exporter_volumes.index)

        # Process any remaining volume
        remaining_demand -= eu_export_volumes.sum()
        if (remaining_demand > 0) and ((exporter_volumes - eu_export_volumes).max() >= remaining_demand):
            # valid_exporters = those that can still cover the leftover
            valid_exporters = (exporter_volumes - eu_export_volumes) >= remaining_demand
            # pick one final exporter, weighted by volumes they have left
            final_exporter = np.random.choice(
                exporter_volumes[valid_exporters].index,
                p=(exporter_volumes[valid_exporters] / exporter_volumes[valid_exporters].sum())
            )
            eu_export_volumes.loc[final_exporter] += remaining_demand

        #
        # 3. Pre-aggregate relationships for traceability.
        #
        #    (a) Exporter -> (middleman, volume)
        #    (b) Middleman -> [list of unique FARMERs feeding that middleman]
        #

        # (a) For each exporter, find the middlemen (source) and their total volume
        exporter_middleman_agg = (
            self.transactions[exporter_mask]
            .groupby(['target', 'source'])['volume']
            .sum()
            .reset_index()
            .rename(columns={'target': 'exporter', 'source': 'middleman', 'volume': 'middleman_volume'})
        )
        # Sort each exporter's middlemen by volume descending
        # We'll keep them in a grouped DataFrame so we can easily slice top-X later
        exporter_middleman_agg_sorted = (
            exporter_middleman_agg
            .sort_values(['exporter', 'middleman_volume'], ascending=[True, False])
            .reset_index(drop=True)
        )

        # (b) For each middleman, find all unique FARMERs that feed that middleman
        farmer_mask = self.transactions['source'].str.contains('FARMER')
        farmer_df = self.transactions[farmer_mask].copy()  # (farmer -> middleman)
        # group by 'target' => that 'target' is the middleman
        middleman_to_farmers = (
            farmer_df.groupby('target')['source']
            .apply(lambda x: x.unique())  # array of farmer IDs
            .to_dict()
        )

        #
        # 4. Build final export list with traceability
        #
        exporters_with_exports = eu_export_volumes[eu_export_volumes > 0.0].index
        eu_exports = []

        traceability_rate = self.config.traceability_rate
        for exporter in exporters_with_exports:
            export_volume = eu_export_volumes.loc[exporter]

            # If traceability_rate == 0, we gather *all* farmers who feed any middleman of this exporter
            if traceability_rate == 0:
                # In that case, we can simply grab all middlemen for the exporter and union all farmers
                all_middlemen = exporter_middleman_agg_sorted.loc[
                    exporter_middleman_agg_sorted['exporter'] == exporter, 'middleman'
                ].values

                # Gather all farmers from those middlemen
                farmer_ids = []
                for m in all_middlemen:
                    farmer_ids.extend(middleman_to_farmers.get(m, []))
                # Sort & unique them
                farmer_ids = sorted(set(farmer_ids))

            else:
                # If traceability_rate > 0, select the top X middlemen by volume
                exporter_df = exporter_middleman_agg_sorted[
                    exporter_middleman_agg_sorted['exporter'] == exporter
                ]
                num_middlemen = max(1, int(len(exporter_df) * traceability_rate))
                selected_middlemen = exporter_df['middleman'].iloc[:num_middlemen].values

                # Union all farmers from these selected middlemen
                farmer_ids = []
                for m in selected_middlemen:
                    farmer_ids.extend(middleman_to_farmers.get(m, []))
                # Sort & unique
                farmer_ids = sorted(set(farmer_ids))

            # Record the final export row
            eu_exports.append({
                'source': exporter,
                'volume': export_volume,
                'farmer_ids': farmer_ids
            })

        total_time = time.time() - start_time
        print(f"Total EU export simulation time: {total_time:.2f}s")

        return eu_exports


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
