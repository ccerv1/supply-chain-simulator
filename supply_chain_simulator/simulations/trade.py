"""
Trading Relationships Generator

This module handles the creation and management of trading relationships between
different actors in the coffee supply chain (farmers, middlemen, and exporters).
"""

import numpy as np
from typing import List, Dict, Tuple, Optional
import ast
from collections import defaultdict
import logging

from models.actors import Farmer, Middleman, Exporter
from models.geography import Country, Geography
from supply_chain_simulator.models.trade_flow import TradeFlow
from database.manager import DatabaseManager
from database.registries import (
    CountryRegistry,
    GeographyRegistry,
    FarmerRegistry,
    MiddlemanRegistry,
    ExporterRegistry,
    TradingRegistry
)
from simulations.middleman_geographies import assign_middlemen_to_geographies
from config.simulation import (
    DEFAULT_RANDOM_SEED,
    MAX_BUYERS_PER_FARMER,
    LOGNORMAL_ADJUSTMENT
)

logger = logging.getLogger(__name__)

class TradeSimulator:
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.country_registry = CountryRegistry(db_manager)
        self.geography_registry = GeographyRegistry(db_manager)
        self.farmer_registry = FarmerRegistry(db_manager)
        self.middleman_registry = MiddlemanRegistry(db_manager)
        self.exporter_registry = ExporterRegistry(db_manager)
        self.trading_registry = TradingRegistry(db_manager)
        np.random.seed(DEFAULT_RANDOM_SEED)

    def simulate_trade_flows(
        self,
        country: Country,
        farmers: List[Farmer],
        middlemen: List[Middleman],
        exporters: List[Exporter],
        middleman_geographies: Dict[str, List[str]],
        year: int = 0
    ) -> List[TradeFlow]:
        """Create initial trading relationships between actors."""
        
        if not middleman_geographies:
            raise ValueError("No middleman geography assignments provided")

        # Create geo-to-middlemen mapping
        geo_to_middlemen = defaultdict(list)
        for mm_id, geo_ids in middleman_geographies.items():
            mm = next(m for m in middlemen if m.id == mm_id)
            for geo_id in geo_ids:
                geo_to_middlemen[geo_id].append(mm)

        # Assign middlemen to exporters
        mm_to_exporters = self._assign_middlemen_to_exporters(
            middlemen=middlemen,
            exporters=exporters,
            country=country
        )

        # Assign farmers to middlemen
        farmer_to_middlemen = self._assign_farmers_to_middlemen(
            farmers=farmers,
            geo_to_middlemen=geo_to_middlemen,
            country=country
        )

        # Generate relationships with volumes
        return self._generate_relationships(
            year=year,
            country=country,
            farmer_to_middlemen=farmer_to_middlemen,
            mm_to_exporters=mm_to_exporters,
            farmers=farmers
        )

    def _assign_middlemen_to_exporters(
        self,
        middlemen: List[Middleman],
        exporters: List[Exporter],
        country: Country
    ) -> Dict[str, List[Exporter]]:
        """Vectorized assignment of exporters to middlemen."""
        mm_to_exporters = {}
        
        # Convert to numpy arrays for vectorized operations
        loyalties = np.array([mm.loyalty for mm in middlemen])
        competitiveness = np.array([e.competitiveness for e in exporters])
        
        # Vectorized calculation of number of exporters per middleman
        num_exporters_per_mm = np.maximum(1, np.round(
            country.max_exporters_per_middleman * (1 - loyalties**2)
        )).astype(int)
        
        # Normalize competitiveness for probability calculation
        probs = competitiveness / competitiveness.sum()
        
        # Vectorized assignment
        for mm, num_exp in zip(middlemen, num_exporters_per_mm):
            chosen_exporters = np.random.choice(
                exporters,
                size=min(num_exp, len(exporters)),
                p=probs,
                replace=False
            )
            mm_to_exporters[mm.id] = list(chosen_exporters)
        
        return mm_to_exporters

    def _assign_farmers_to_middlemen(
        self,
        farmers: List[Farmer],
        geo_to_middlemen: Dict[str, List[Middleman]],
        country: Country
    ) -> Dict[str, List[Middleman]]:
        """Vectorized assignment of middlemen to farmers."""
        farmer_to_middlemen = {}
        
        # Group farmers by geography for batch processing
        geo_farmers = defaultdict(list)
        for farmer in farmers:
            geo_farmers[farmer.geography_id].append(farmer)
        
        # Process each geography batch
        for geo_id, geo_farmers_list in geo_farmers.items():
            available_middlemen = geo_to_middlemen.get(geo_id, [])
            if not available_middlemen:
                continue
            
            # Vectorized loyalty calculations
            loyalties = np.array([f.loyalty for f in geo_farmers_list])
            num_buyers = np.maximum(1, np.round(
                MAX_BUYERS_PER_FARMER * (1 - loyalties**2)
            )).astype(int)
            
            # Batch assignment for all farmers in this geography
            for farmer, num_mm in zip(geo_farmers_list, num_buyers):
                chosen_middlemen = list(np.random.choice(
                    available_middlemen,
                    size=min(num_mm, len(available_middlemen)),
                    replace=False
                ))
                farmer_to_middlemen[farmer.id] = chosen_middlemen
        
        return farmer_to_middlemen

    def _generate_relationships(
        self,
        year: int,
        country: Country,
        farmer_to_middlemen: Dict[str, List[Middleman]],
        mm_to_exporters: Dict[str, List[Exporter]],
        farmers: List[Farmer]
    ) -> List[TradeFlow]:
        """Generate relationships using vectorized operations where possible."""
        relationships = []
        total_eu_volume = 0
        
        # Pre-calculate all random splits using numpy
        farmer_splits = {
            f.id: np.random.dirichlet(np.ones(len(farmer_to_middlemen[f.id])))
            for f in farmers if f.id in farmer_to_middlemen
        }
        
        mm_splits = {
            mm_id: np.random.dirichlet(np.ones(len(exporters)))
            for mm_id, exporters in mm_to_exporters.items()
        }
        
        # Vectorized EU sales probability calculation
        eu_probs = np.random.random(len(farmers))
        
        # Process relationships in batches
        for farmer in farmers:
            if farmer.id not in farmer_to_middlemen:
                continue
            
            mm_list = farmer_to_middlemen[farmer.id]
            mm_split = farmer_splits[farmer.id]
            
            for mm, mm_ratio in zip(mm_list, mm_split):
                mm_volume = farmer.production_amount * mm_ratio
                exporters = mm_to_exporters[mm.id]
                exp_split = mm_splits[mm.id]
                
                for exp, exp_ratio in zip(exporters, exp_split):
                    exp_volume = mm_volume * exp_ratio
                    if exp_volume < 1:
                        continue
                    
                    # EU sales determination
                    sold_to_eu = False
                    if total_eu_volume < country.exports_to_eu:
                        eu_threshold = 1 - (country.traceability_rate * exp.eu_preference)
                        if np.random.random() > eu_threshold:
                            sold_to_eu = True
                            total_eu_volume += exp_volume
                    
                    relationships.append(TradeFlow(
                        year=year,
                        country_id=country.id,
                        farmer_id=farmer.id,
                        middleman_id=mm.id,
                        exporter_id=exp.id,
                        amount_kg=int(exp_volume),
                        sold_to_eu=sold_to_eu
                    ))
        
        # Vectorized volume adjustments
        self._adjust_volumes(relationships, country)
        
        return relationships

    def _adjust_volumes(self, relationships: List[TradeFlow], country: Country):
        """Vectorized volume adjustment."""
        eu_relationships = np.array([r for r in relationships if r.sold_to_eu])
        non_eu_relationships = np.array([r for r in relationships if not r.sold_to_eu])
        
        if len(eu_relationships) > 0:
            current_eu_volume = sum(r.amount_kg for r in eu_relationships)
            volume_difference = country.exports_to_eu - current_eu_volume
            largest_eu_idx = np.argmax([r.amount_kg for r in eu_relationships])
            eu_relationships[largest_eu_idx].amount_kg += volume_difference
        
        non_eu_target = country.total_production - country.exports_to_eu
        if len(non_eu_relationships) > 0:
            current_non_eu_volume = sum(r.amount_kg for r in non_eu_relationships)
            volume_difference = non_eu_target - current_non_eu_volume
            largest_non_eu_idx = np.argmax([r.amount_kg for r in non_eu_relationships])
            non_eu_relationships[largest_non_eu_idx].amount_kg += volume_difference
