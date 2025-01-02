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
from models.relationships import TradingRelationship
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

    def create_initial_relationships(
        self,
        country: Country,
        farmers: List[Farmer],
        middlemen: List[Middleman],
        exporters: List[Exporter],
        geographies: List[Geography]
    ) -> List[TradingRelationship]:
        """Create initial trading relationships between supply chain actors."""
        
        # 1. Assign middlemen to geographies
        geo_to_middlemen = assign_middlemen_to_geographies(geographies, middlemen)
        middleman_to_geos = {m.id: [] for m in middlemen}
        for geo_id, mm_list in geo_to_middlemen.items():
            for mm in mm_list:
                middleman_to_geos[mm.id].append(geo_id)

        # 2. Create middleman-exporter relationships
        mm_to_exporters = self._assign_middlemen_to_exporters(
            middlemen=middlemen,
            exporters=exporters,
            middleman_to_geos=middleman_to_geos,
            country=country
        )

        # 3. Create farmer-middleman relationships
        farmer_to_middlemen = self._assign_farmers_to_middlemen(
            farmers=farmers,
            geo_to_middlemen=geo_to_middlemen,
            country=country
        )

        # 4. Generate all relationships with volumes
        relationships = self._generate_trading_relationships(
            country=country,
            farmer_to_middlemen=farmer_to_middlemen,
            mm_to_exporters=mm_to_exporters,
            farmers=farmers
        )

        return relationships

    def _assign_middlemen_to_exporters(
        self,
        middlemen: List[Middleman],
        exporters: List[Exporter],
        middleman_to_geos: Dict[str, List[str]],
        country: Country
    ) -> Dict[str, List[Exporter]]:
        """Assign exporters to middlemen based on loyalty and geography coverage."""
        mm_to_exporters = {}
        
        for mm in middlemen:
            # Modified: Use square of (1-loyalty) to skew towards maximum exporters
            # This makes higher numbers of exporters more likely
            num_exporters = max(1, round(
                country.max_exporters_per_middleman * (1 - mm.loyalty**2)
            ))
            
            # Weight exporters by competitiveness and middleman's geography coverage
            geo_coverage = len(middleman_to_geos[mm.id])
            exporter_scores = [
                e.competitiveness * (1 + geo_coverage/len(exporters))
                for e in exporters
            ]
            probs = np.array(exporter_scores) / sum(exporter_scores)
            
            chosen_exporters = list(np.random.choice(
                exporters,
                size=num_exporters,
                p=probs,
                replace=False
            ))
            
            mm_to_exporters[mm.id] = chosen_exporters
        
        return mm_to_exporters

    def _assign_farmers_to_middlemen(
        self,
        farmers: List[Farmer],
        geo_to_middlemen: Dict[str, List[Middleman]],
        country: Country
    ) -> Dict[str, List[Middleman]]:
        """Assign middlemen to farmers based on loyalty and geography."""
        farmer_to_middlemen = {}
        
        for farmer in farmers:
            available_middlemen = geo_to_middlemen.get(farmer.geography_id, [])
            if not available_middlemen:
                continue
            
            # Number of buyers based on loyalty
            num_buyers = max(1, round(
                country.max_buyers_per_farmer * (1 - farmer.loyalty**2)
            ))
            num_buyers = min(num_buyers, len(available_middlemen))
            
            chosen_middlemen = list(np.random.choice(
                available_middlemen,
                size=num_buyers,
                replace=False
            ))
            
            farmer_to_middlemen[farmer.id] = chosen_middlemen
        
        return farmer_to_middlemen

    def _generate_trading_relationships(
        self,
        country: Country,
        farmer_to_middlemen: Dict[str, List[Middleman]],
        mm_to_exporters: Dict[str, List[Exporter]],
        farmers: List[Farmer]
    ) -> List[TradingRelationship]:
        """Generate final relationships with volumes and EU sales."""
        relationships = []
        total_eu_volume = 0
        
        # Add logging for initial targets
        logger.info(f"Target total production: {country.total_production:,} kg")
        logger.info(f"Target EU exports: {country.exports_to_eu:,} kg")
        
        # Process each farmer's production
        for farmer in farmers:
            if farmer.id not in farmer_to_middlemen:
                continue
            
            # Split farmer's production among middlemen
            mm_list = farmer_to_middlemen[farmer.id]
            mm_split = np.random.dirichlet(np.ones(len(mm_list)))
            
            # For each middleman, create relationships with their exporters
            for mm, mm_ratio in zip(mm_list, mm_split):
                mm_volume = farmer.production_amount * mm_ratio
                exporters = mm_to_exporters[mm.id]
                
                # Split middleman's volume among exporters
                exp_split = np.random.dirichlet(np.ones(len(exporters)))
                
                for exp, exp_ratio in zip(exporters, exp_split):
                    exp_volume = mm_volume * exp_ratio
                    
                    # Determine EU sales based on traceability rate
                    sold_to_eu = False
                    if total_eu_volume < country.exports_to_eu:
                        eu_threshold = 1 - (country.traceability_rate * exp.eu_preference)
                        if np.random.random() > eu_threshold:
                            sold_to_eu = True
                            total_eu_volume += exp_volume

                    relationships.append(TradingRelationship(
                        year=0,
                        country_id=country.id,
                        farmer_id=farmer.id,
                        middleman_id=mm.id,
                        exporter_id=exp.id,
                        amount_kg=int(exp_volume),
                        sold_to_eu=sold_to_eu
                    ))

        # Final scaling to match country totals
        eu_relationships = [r for r in relationships if r.sold_to_eu]
        non_eu_relationships = [r for r in relationships if not r.sold_to_eu]
        
        # Adjust EU volumes by modifying largest sale
        if eu_relationships:
            current_eu_volume = sum(r.amount_kg for r in eu_relationships)
            volume_difference = country.exports_to_eu - current_eu_volume
            largest_eu_rel = max(eu_relationships, key=lambda r: r.amount_kg)
            largest_eu_rel.amount_kg += volume_difference

        # Adjust non-EU volumes by modifying largest sale
        non_eu_target = country.total_production - country.exports_to_eu
        if non_eu_relationships:
            current_non_eu_volume = sum(r.amount_kg for r in non_eu_relationships)
            volume_difference = non_eu_target - current_non_eu_volume
            largest_non_eu_rel = max(non_eu_relationships, key=lambda r: r.amount_kg)
            largest_non_eu_rel.amount_kg += volume_difference

        # After scaling, add verification logging
        final_eu_volume = sum(r.amount_kg for r in relationships if r.sold_to_eu)
        final_total_volume = sum(r.amount_kg for r in relationships)
        
        logger.info(f"Final EU volume: {final_eu_volume:,} kg")
        logger.info(f"Final total volume: {final_total_volume:,} kg")
        
        # Add validation checks
        if abs(final_eu_volume - country.exports_to_eu) > 1:
            logger.warning(f"EU volume mismatch: {final_eu_volume:,} vs target {country.exports_to_eu:,}")
        if abs(final_total_volume - country.total_production) > 1:
            logger.warning(f"Total volume mismatch: {final_total_volume:,} vs target {country.total_production:,}")

        return relationships

