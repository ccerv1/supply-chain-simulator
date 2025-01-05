"""Trade flow simulation with efficient vectorized operations."""

import numpy as np
import pandas as pd
from typing import List, Dict, Optional
import logging
from collections import defaultdict

from models.actors import Farmer, Middleman, Exporter
from models.geography import Country
from models.trade_flow import TradeFlow
from database.registries import (
    TradingRegistry, MiddlemanGeographyRegistry, 
    FarmerMiddlemanRegistry, MiddlemanExporterRegistry
)

logger = logging.getLogger(__name__)

class TradeSimulator:
    def __init__(self, db_manager):
        self.db = db_manager
        self.trading_registry = TradingRegistry(db_manager)
        self.mm_geo_registry = MiddlemanGeographyRegistry(db_manager)
        self.farmer_mm_registry = FarmerMiddlemanRegistry(db_manager)
        self.mm_exp_registry = MiddlemanExporterRegistry(db_manager)

    def simulate_trade_flows(self, country: Country, farmers: List[Farmer],
                           middlemen: List[Middleman], exporters: List[Exporter],
                           year: int = 0) -> List[TradeFlow]:
        """Simulate trade flows using vectorized operations."""
        try:
            # Get current network state if exists
            network_df = self._get_network_state(year, country.id)
            
            if year == 0 or network_df.empty:
                # Initial assignment
                network_df = self._create_initial_network(
                    country, farmers, middlemen, exporters
                )
            else:
                # Update relationships based on loyalty
                network_df = self._update_network_relationships(
                    network_df, country
                )
            
            # Calculate volumes and EU sales
            flows_df = self._calculate_trade_flows(
                network_df, country, year
            )
            
            # Convert to TradeFlow objects and save
            trade_flows = self._create_trade_flows(flows_df)
            self.trading_registry.create_many(trade_flows)
            
            return trade_flows
            
        except Exception as e:
            logger.error(f"Error simulating trade flows: {str(e)}")
            raise

    def _get_network_state(self, year: int, country_id: str) -> pd.DataFrame:
        """Get current network state as DataFrame."""
        network = self.trading_registry.get_active_trading_network(year, country_id)
        return pd.DataFrame(network)

    def _create_initial_network(self, country: Country, 
                              farmers: List[Farmer],
                              middlemen: List[Middleman],
                              exporters: List[Exporter]) -> pd.DataFrame:
        """Create initial network relationships."""
        # Create farmer-middleman assignments
        farmer_mm = self._assign_farmers_to_middlemen(farmers, middlemen)
        
        # Create middleman-exporter assignments
        mm_exp = self._assign_middlemen_to_exporters(middlemen, exporters)
        
        # Merge into complete network
        network_df = pd.merge(
            farmer_mm, mm_exp,
            on='middleman_id',
            how='inner'
        )
        
        # Save new relationships
        self.farmer_mm_registry.create_many(
            farmer_mm.to_dict('records'), year=0
        )
        self.mm_exp_registry.create_many(
            mm_exp.to_dict('records'), year=0
        )
        
        return network_df

    def _update_network_relationships(self, network_df: pd.DataFrame, 
                                   country: Country) -> pd.DataFrame:
        """Update relationships based on loyalty scores."""
        # Calculate switch probabilities
        switch_probs = 1 - network_df[['farmer_loyalty', 'middleman_loyalty']]
        
        # Identify relationships to change
        to_switch = (np.random.random(len(network_df)) < switch_probs).any(axis=1)
        
        if not to_switch.any():
            return network_df
            
        # Reassign relationships that need changing
        new_assignments = self._reassign_relationships(
            network_df[to_switch], country
        )
        
        # Combine unchanged and new relationships
        return pd.concat([
            network_df[~to_switch],
            new_assignments
        ])

    def _calculate_trade_flows(self, network_df: pd.DataFrame,
                             country: Country,
                             year: int) -> pd.DataFrame:
        """Calculate volumes and EU sales for trade flows."""
        # Calculate base volumes using vectorized operations
        volumes = self._calculate_volumes(network_df, country)
        
        # Determine EU sales
        eu_sales = self._determine_eu_sales(
            volumes, network_df['eu_preference'], country
        )
        
        # Create flows DataFrame
        flows_df = pd.DataFrame({
            'year': year,
            'country_id': country.id,
            'farmer_id': network_df['farmer_id'],
            'middleman_id': network_df['middleman_id'],
            'exporter_id': network_df['exporter_id'],
            'amount_kg': volumes,
            'sold_to_eu': eu_sales
        })
        
        return flows_df

    def _create_trade_flows(self, flows_df: pd.DataFrame) -> List[TradeFlow]:
        """Convert DataFrame to TradeFlow objects."""
        return [
            TradeFlow(**row) 
            for row in flows_df.to_dict('records')
        ]

    # Helper methods for relationship assignments and volume calculations...
