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
import random

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
    TradingRegistry,
    MiddlemanGeographyRegistry,
    FarmerMiddlemanRegistry,
    MiddlemanExporterRegistry
)
from simulations.middleman_geographies import assign_middlemen_to_geographies
from config.simulation import (
    DEFAULT_RANDOM_SEED,
    MAX_BUYERS_PER_FARMER
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
        self.mm_geo_registry = MiddlemanGeographyRegistry(db_manager)
        self.farmer_mm_registry = FarmerMiddlemanRegistry(db_manager)
        self.mm_exp_registry = MiddlemanExporterRegistry(db_manager)
        
        np.random.seed(DEFAULT_RANDOM_SEED)

    def simulate_trade_flows(self, country: Country, farmers: List[Farmer],
                            middlemen: List[Middleman], exporters: List[Exporter],
                            year: int = 0) -> List[TradeFlow]:
        """Create trading relationships between actors."""
        try:
            # Get active relationships for this year
            mm_geo_rels = self.mm_geo_registry.get_active_relationships(year, country.id)
            farmer_mm_rels = self.farmer_mm_registry.get_active_relationships(year, country.id)
            mm_exp_rels = self.mm_exp_registry.get_active_relationships(year, country.id)
            
            # Process in a single transaction
            with self.db.transaction():
                if year == 0:
                    # Initial relationships
                    mm_exp_rels = self._assign_middlemen_to_exporters(middlemen, exporters, country)
                    self.mm_exp_registry.create_many(mm_exp_rels, year)
                    
                    farmer_mm_rels = self._assign_farmers_to_middlemen(farmers, mm_geo_rels, country)
                    self.farmer_mm_registry.create_many(farmer_mm_rels, year)
                else:
                    # Update existing relationships
                    self._update_farmer_middleman_relationships(
                        farmers, middlemen, farmer_mm_rels, mm_geo_rels, country, year
                    )
                    self._update_middleman_exporter_relationships(
                        middlemen, exporters, mm_exp_rels, country, year
                    )
                    
                    # Get updated relationships
                    farmer_mm_rels = self.farmer_mm_registry.get_active_relationships(year, country.id)
                    mm_exp_rels = self.mm_exp_registry.get_active_relationships(year, country.id)

            # Generate trade flows based on relationships
            return self._generate_relationships(
                year=year,
                country=country,
                farmers=farmers,
                farmer_mm_rels=farmer_mm_rels,
                mm_exp_rels=mm_exp_rels
            )
        except Exception as e:
            logger.error(f"Error in simulate_trade_flows: {str(e)}")
            raise

    def _update_farmer_middleman_relationships(
        self, farmers: List[Farmer], middlemen: List[Middleman], 
        current_rels: List[Dict], mm_geo_rels: List[Dict],
        country: Country, year: int
    ):
        """Update farmer-middleman relationships based on loyalty."""
        relationships_to_end = []
        new_relationships = []
        
        for farmer in farmers:
            if random.random() < (1 - farmer.loyalty) * country.farmer_switch_rate:
                # Get current middlemen
                current_mms = [
                    rel['middleman_id'] for rel in current_rels 
                    if rel['farmer_id'] == farmer.id
                ]
                
                # Get available middlemen in farmer's geography
                available_mms = [
                    rel['middleman_id'] for rel in mm_geo_rels 
                    if rel['geography_id'] == farmer.geography_id
                    and rel['middleman_id'] not in current_mms
                ]
                
                if current_mms and available_mms:
                    # End one relationship
                    old_mm = random.choice(current_mms)
                    relationships_to_end.append((farmer.id, old_mm))
                    
                    # Start a new one - Changed to dictionary format
                    new_mm = random.choice(available_mms)
                    new_relationships.append({
                        'farmer_id': farmer.id,
                        'middleman_id': new_mm
                    })
        
        # Apply changes
        if relationships_to_end:
            self.farmer_mm_registry.end_relationships(relationships_to_end, year)
        if new_relationships:
            self.farmer_mm_registry.create_many(new_relationships, year)

    def _update_middleman_exporter_relationships(
        self, middlemen: List[Middleman], exporters: List[Exporter],
        current_rels: List[Dict], country: Country, year: int
    ):
        """Update middleman-exporter relationships based on loyalty."""
        relationships_to_end = []
        new_relationships = []
        
        for middleman in middlemen:
            if random.random() < (1 - middleman.loyalty) * country.middleman_switch_rate:
                # Get current and available exporters
                current_exps = [
                    rel['exporter_id'] for rel in current_rels 
                    if rel['middleman_id'] == middleman.id
                ]
                available_exps = [e.id for e in exporters if e.id not in current_exps]
                
                if current_exps and available_exps:
                    # End one relationship
                    old_exp = random.choice(current_exps)
                    relationships_to_end.append((middleman.id, old_exp))
                    
                    # Start a new one - Changed to dictionary format
                    new_exp = random.choice(available_exps)
                    new_relationships.append({
                        'middleman_id': middleman.id,
                        'exporter_id': new_exp
                    })
        
        # Apply changes
        if relationships_to_end:
            self.mm_exp_registry.end_relationships(relationships_to_end, year)
        if new_relationships:
            self.mm_exp_registry.create_many(new_relationships, year)

    def _assign_middlemen_to_exporters(
        self,
        middlemen: List[Middleman],
        exporters: List[Exporter],
        country: Country
    ) -> List[Dict]:
        """Vectorized assignment of exporters to middlemen."""
        relationships = []
        
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
            # Create relationships directly
            for exporter in chosen_exporters:
                relationships.append({
                    'middleman_id': mm.id,
                    'exporter_id': exporter.id
                })
        
        return relationships

    def _assign_farmers_to_middlemen(
        self,
        farmers: List[Farmer],
        mm_geo_rels: List[Dict],
        country: Country
    ) -> List[Dict]:
        """Vectorized assignment of middlemen to farmers."""
        relationships = []
        
        # Group farmers by geography for batch processing
        geo_farmers = defaultdict(list)
        for farmer in farmers:
            geo_farmers[farmer.geography_id].append(farmer)
        
        # Process each geography batch
        for geo_id, geo_farmers_list in geo_farmers.items():
            available_middlemen = [
                rel['middleman_id'] for rel in mm_geo_rels 
                if rel['geography_id'] == geo_id
            ]
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
                # Create relationships directly
                for middleman_id in chosen_middlemen:
                    relationships.append({
                        'farmer_id': farmer.id,
                        'middleman_id': middleman_id
                    })
        
        return relationships

    def _generate_relationships(self, year: int, country: Country, farmers: List[Farmer],
                              farmer_mm_rels: List[Dict], mm_exp_rels: List[Dict]) -> List[TradeFlow]:
        """Generate trade flows more efficiently."""
        # Pre-process all lookups
        relationships = []
        exporter_lookup = {e.id: e for e in self.exporter_registry.get_by_country(country.id)}
        farmer_mm_map = defaultdict(list)
        mm_exp_map = defaultdict(list)
        
        # Build relationship maps once
        for rel in farmer_mm_rels:
            farmer_mm_map[rel['farmer_id']].append(rel['middleman_id'])
        for rel in mm_exp_rels:
            mm_exp_map[rel['middleman_id']].append(rel['exporter_id'])
        
        # Process in batches
        for farmer in farmers:
            if farmer.id not in farmer_mm_map:
                continue
            
            flows = self._generate_farmer_flows(
                farmer=farmer,
                middleman_ids=farmer_mm_map[farmer.id], 
                mm_exp_map=mm_exp_map,
                exporter_lookup=exporter_lookup,
                country=country,
                year=year
            )
            relationships.extend(flows)
        
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

    def _generate_farmer_flows(self, farmer: Farmer, middleman_ids: List[str], 
                             mm_exp_map: Dict, exporter_lookup: Dict, country: Country, year: int) -> List[TradeFlow]:
        """Generate trade flows for a single farmer."""
        flows_dict = {}  # Use dict to prevent duplicates
        total_eu_volume = 0
        
        # Pre-calculate EU ratio for this farmer's production
        eu_ratio = country.exports_to_eu / country.total_production
        
        # Split farmer's production among middlemen
        mm_split = np.random.dirichlet(np.ones(len(middleman_ids)))
        
        for mm_id, mm_ratio in zip(middleman_ids, mm_split):
            mm_volume = farmer.production_amount * mm_ratio
            
            # Get this middleman's exporters
            exporter_ids = mm_exp_map.get(mm_id, [])
            if not exporter_ids:
                continue
            
            # Split middleman's volume among exporters
            exp_split = np.random.dirichlet(np.ones(len(exporter_ids)))
            
            for exp_id, exp_ratio in zip(exporter_ids, exp_split):
                exp_volume = mm_volume * exp_ratio
                if exp_volume < 1:
                    continue
                
                exporter = exporter_lookup[exp_id]
                
                # Enhanced EU sales determination
                sold_to_eu = False
                if total_eu_volume < country.exports_to_eu:
                    # Calculate probability based on EU ratio and exporter preference
                    probability_to_eu = min(
                        exporter.eu_preference,
                        eu_ratio * (1 + 0.1 * (exporter.eu_preference - 0.5))
                    )
                    
                    if np.random.random() < probability_to_eu:
                        sold_to_eu = True
                        total_eu_volume += exp_volume
                
                # Create unique key for this flow
                flow_key = (year, country.id, farmer.id, mm_id, exp_id, sold_to_eu)
                
                # If flow exists, add to volume, otherwise create new flow
                if flow_key in flows_dict:
                    flows_dict[flow_key].amount_kg += int(exp_volume)
                else:
                    flows_dict[flow_key] = TradeFlow(
                        year=year,
                        country_id=country.id,
                        farmer_id=farmer.id,
                        middleman_id=mm_id,
                        exporter_id=exp_id,
                        amount_kg=int(exp_volume),
                        sold_to_eu=sold_to_eu
                    )
        
        return list(flows_dict.values())
