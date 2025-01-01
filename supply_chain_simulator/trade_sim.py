"""
Trading Relationships Generator

This module handles the creation and management of trading relationships between
different actors in the coffee supply chain (farmers, middlemen, and exporters).
"""

import numpy as np
from typing import List, Dict
import ast
from scipy.spatial.distance import cdist

from models import Country, Farmer, Middleman, Exporter, Geography

def create_trading_relationships(
    country: Country,
    farmers: List[Farmer],
    middlemen: List[Middleman],
    exporters: List[Exporter],
    geographies: List[Geography]
) -> List[dict]:
    """Create initial trading relationships between supply chain actors."""
    
    # Convert centroids from string to coordinates
    geo_centroids = np.array([
        ast.literal_eval(g.centroid) for g in geographies
    ])
    
    # Calculate production per geography for weighting
    geo_production = {g.id: g.total_production_kg for g in geographies}
    
    # Create geographic zones based on proximity
    num_zones = min(len(middlemen) // 4, len(geographies))
    
    # Group geographies into zones using proximity
    zones = {}
    for i in range(num_zones):
        zones[i] = {
            'geographies': [],
            'total_production': 0,
            'middlemen': []
        }
    
    # Assign geographies to nearest zone
    for g in geographies:
        centroid = np.array(ast.literal_eval(g.centroid)).reshape(1, -1)
        distances = cdist(centroid, geo_centroids)
        closest_zone = distances.argmin() % num_zones
        zones[closest_zone]['geographies'].append(g.id)
        zones[closest_zone]['total_production'] += g.total_production_kg
    
    # Calculate zone centroids
    zone_centroids = {}
    for zone_idx, zone in zones.items():
        zone_geo_centroids = np.array([
            ast.literal_eval(g.centroid) 
            for g in geographies 
            if g.id in zone['geographies']
        ])
        zone_centroids[zone_idx] = zone_geo_centroids.mean(axis=0)
    
    # Assign middlemen to zones
    total_production = sum(zone['total_production'] for zone in zones.values())
    for zone_idx, zone in zones.items():
        base_middlemen = max(
            2,
            int(len(middlemen) * (zone['total_production'] / total_production))
        )
        
        target_middlemen = int(base_middlemen * (1 + np.random.uniform(-0.3, 0.3)))
        target_middlemen = min(max(2, target_middlemen), len(middlemen) // 2)
        
        # Select middlemen based on competitiveness
        available_middlemen = [m for m in middlemen if not any(m in z['middlemen'] for z in zones.values())]
        if available_middlemen:
            scores = [m.competitiveness for m in available_middlemen]
            probs = np.array(scores) / sum(scores)
            chosen = np.random.choice(
                available_middlemen,
                size=min(target_middlemen, len(available_middlemen)),
                p=probs,
                replace=False
            )
            zones[zone_idx]['middlemen'].extend(chosen)
    
    # Create mapping of geography to available middlemen
    geo_to_middlemen = {}
    for zone_idx, zone in zones.items():
        for geo_id in zone['geographies']:
            geo_to_middlemen[geo_id] = zone['middlemen']
    
    # Create trading relationships
    relationships = []
    remaining_eu_exports = country.exports_to_eu
    
    for farmer in farmers:
        available_middlemen = geo_to_middlemen[farmer.geography.id]
        if not available_middlemen:
            continue
            
        # Select middlemen based on competitiveness and loyalty
        num_buyers = min(
            len(available_middlemen),
            country.max_buyers_per_farmer
        )
        
        scores = [
            m.competitiveness * (1 + farmer.middleman_loyalty * m.farmer_loyalty)
            for m in available_middlemen
        ]
        probs = np.array(scores) / sum(scores)
        
        chosen_middlemen = np.random.choice(
            available_middlemen,
            size=num_buyers,
            p=probs,
            replace=False
        )
        
        # Distribute farmer's production
        amounts = np.random.dirichlet(np.ones(num_buyers)) * farmer.production_amount
        amounts = np.round(amounts).astype(int)
        amounts[-1] = farmer.production_amount - amounts[:-1].sum()
        
        for mm, amount in zip(chosen_middlemen, amounts):
            if amount <= 0:
                continue
                
            # Select exporters
            num_exporters = min(
                country.max_exporters_per_middleman,
                len(exporters)
            )
            
            exporter_scores = [
                e.competitiveness * (1 + mm.exporter_loyalty * e.middleman_loyalty)
                for e in exporters
            ]
            exporter_probs = np.array(exporter_scores) / sum(exporter_scores)
            
            chosen_exporters = np.random.choice(
                exporters,
                size=num_exporters,
                p=exporter_probs,
                replace=False
            )
            
            # Distribute amount among exporters
            exp_amounts = np.random.dirichlet(np.ones(num_exporters)) * amount
            exp_amounts = np.round(exp_amounts).astype(int)
            exp_amounts[-1] = amount - exp_amounts[:-1].sum()
            
            for exp, exp_amount in zip(chosen_exporters, exp_amounts):
                if exp_amount <= 0:
                    continue
                    
                # Determine EU exports
                sold_to_eu = False
                if remaining_eu_exports > 0 and np.random.random() < exp.eu_preference:
                    sold_to_eu = True
                    remaining_eu_exports -= exp_amount
                
                relationships.append({
                    'year': 0,
                    'country': country.id,
                    'farmer_id': farmer.id,
                    'middleman_id': mm.id,
                    'exporter_id': exp.id,
                    'sold_to_eu': sold_to_eu,
                    'amount_kg': int(exp_amount)
                })
    
    return relationships

def simulate_next_year(
    previous_relationships: List[dict],
    country: Country,
    farmers: List[Farmer],
    middlemen: List[Middleman],
    exporters: List[Exporter],
    geographies: List[Geography],
    year: int
) -> List[dict]:
    """Simulate trading relationships for the next year."""
    
    # Create lookup dictionaries
    farmer_map = {f.id: f for f in farmers}
    middleman_map = {m.id: m for m in middlemen}
    
    # Track existing relationships and volumes
    farmer_middlemen = {}  # farmer_id -> {middleman_id: volume}
    middleman_exporters = {}  # middleman_id -> {exporter_id: volume}
    
    for rel in previous_relationships:
        f_id = rel['farmer_id']
        m_id = rel['middleman_id']
        e_id = rel['exporter_id']
        vol = rel['amount_kg']
        
        if f_id not in farmer_middlemen:
            farmer_middlemen[f_id] = {}
        farmer_middlemen[f_id][m_id] = farmer_middlemen[f_id].get(m_id, 0) + vol
        
        if m_id not in middleman_exporters:
            middleman_exporters[m_id] = {}
        middleman_exporters[m_id][e_id] = middleman_exporters[m_id].get(e_id, 0) + vol
    
    new_relationships = []
    remaining_eu_exports = country.exports_to_eu
    
    for farmer in farmers:
        total_volume = farmer.production_amount
        allocated_volume = 0
        
        # Process existing relationships first
        existing_middlemen = list(farmer_middlemen.get(farmer.id, {}).keys())
        if existing_middlemen:
            # Distribute volume among existing relationships first
            volumes = np.random.dirichlet(np.ones(len(existing_middlemen))) * total_volume
            volumes = np.round(volumes).astype(int)
            volumes[-1] = total_volume - volumes[:-1].sum()  # Ensure total volume is preserved
            
            for m_id, volume in zip(existing_middlemen, volumes):
                middleman = middleman_map[m_id]
                
                # Check loyalty
                loyalty_score = farmer.middleman_loyalty * middleman.farmer_loyalty
                if np.random.random() < loyalty_score:
                    if volume > 0:
                        # Distribute to existing exporters first
                        existing_exporters = list(middleman_exporters.get(m_id, {}).keys())
                        available_exporters = [e for e in exporters if e.id in existing_exporters]
                        
                        if not available_exporters:
                            available_exporters = exporters[:country.max_exporters_per_middleman]
                        
                        num_exporters = min(len(available_exporters), country.max_exporters_per_middleman)
                        
                        exporter_scores = [
                            e.competitiveness * (1 + middleman.exporter_loyalty * e.middleman_loyalty)
                            for e in available_exporters
                        ]
                        exporter_probs = np.array(exporter_scores) / sum(exporter_scores)
                        
                        chosen_exporters = np.random.choice(
                            available_exporters,
                            size=num_exporters,
                            p=exporter_probs,
                            replace=False
                        )
                        
                        exp_volumes = np.random.dirichlet(np.ones(num_exporters)) * volume
                        exp_volumes = np.round(exp_volumes).astype(int)
                        exp_volumes[-1] = volume - exp_volumes[:-1].sum()
                        
                        for exp, exp_volume in zip(chosen_exporters, exp_volumes):
                            if exp_volume <= 0:
                                continue
                                
                            sold_to_eu = False
                            if remaining_eu_exports > 0 and np.random.random() < exp.eu_preference:
                                sold_to_eu = True
                                remaining_eu_exports -= exp_volume
                            
                            new_relationships.append({
                                'year': year,
                                'country': country.id,
                                'farmer_id': farmer.id,
                                'middleman_id': m_id,
                                'exporter_id': exp.id,
                                'sold_to_eu': sold_to_eu,
                                'amount_kg': int(exp_volume)
                            })
                    
                    allocated_volume += volume
        
        # Always process remaining volume with available middlemen
        if allocated_volume < total_volume:
            remaining_volume = total_volume - allocated_volume
            available_middlemen = [m for m in middlemen if m.id not in existing_middlemen]
            
            if available_middlemen:
                remaining_slots = max(1, country.max_buyers_per_farmer - len(existing_middlemen))
                num_new = min(remaining_slots, len(available_middlemen))
                
                # Force at least one new relationship if we have remaining volume
                num_new = max(1, num_new)
                
                scores = [m.competitiveness for m in available_middlemen]
                probs = np.array(scores) / sum(scores)
                
                new_middlemen = np.random.choice(
                    available_middlemen,
                    size=num_new,
                    p=probs,
                    replace=False
                )
                
                # Allocate all remaining volume
                volumes = np.random.dirichlet(np.ones(num_new)) * remaining_volume
                volumes = np.round(volumes).astype(int)
                volumes[-1] = remaining_volume - volumes[:-1].sum()
                
                for mm, volume in zip(new_middlemen, volumes):
                    if volume <= 0:
                        continue
                        
                    # Select exporters for new relationships
                    num_exporters = min(
                        country.max_exporters_per_middleman,
                        len(exporters)
                    )
                    
                    exporter_scores = [
                        e.competitiveness * (1 + mm.exporter_loyalty * e.middleman_loyalty)
                        for e in exporters
                    ]
                    exporter_probs = np.array(exporter_scores) / sum(exporter_scores)
                    
                    chosen_exporters = np.random.choice(
                        exporters,
                        size=num_exporters,
                        p=exporter_probs,
                        replace=False
                    )
                    
                    exp_volumes = np.random.dirichlet(np.ones(num_exporters)) * volume
                    exp_volumes = np.round(exp_volumes).astype(int)
                    exp_volumes[-1] = volume - exp_volumes[:-1].sum()
                    
                    for exp, exp_volume in zip(chosen_exporters, exp_volumes):
                        if exp_volume <= 0:
                            continue
                            
                        sold_to_eu = False
                        if remaining_eu_exports > 0 and np.random.random() < exp.eu_preference:
                            sold_to_eu = True
                            remaining_eu_exports -= exp_volume
                        
                        new_relationships.append({
                            'year': year,
                            'country': country.id,
                            'farmer_id': farmer.id,
                            'middleman_id': mm.id,
                            'exporter_id': exp.id,
                            'sold_to_eu': sold_to_eu,
                            'amount_kg': int(exp_volume)
                        })
            else:
                # If no new middlemen available, distribute remaining volume among existing ones
                volumes = np.random.dirichlet(np.ones(len(existing_middlemen))) * remaining_volume
                volumes = np.round(volumes).astype(int)
                volumes[-1] = remaining_volume - volumes[:-1].sum()
                
                for m_id, volume in zip(existing_middlemen, volumes):
                    if volume <= 0:
                        continue
                        
                    middleman = middleman_map[m_id]
                    
                    # Use existing exporters or find new ones
                    existing_exporters = list(middleman_exporters.get(m_id, {}).keys())
                    available_exporters = [e for e in exporters if e.id in existing_exporters]
                    
                    if not available_exporters:
                        available_exporters = exporters[:country.max_exporters_per_middleman]
                    
                    num_exporters = min(len(available_exporters), country.max_exporters_per_middleman)
                    
                    exporter_scores = [
                        e.competitiveness * (1 + middleman.exporter_loyalty * e.middleman_loyalty)
                        for e in available_exporters
                    ]
                    exporter_probs = np.array(exporter_scores) / sum(exporter_scores)
                    
                    chosen_exporters = np.random.choice(
                        available_exporters,
                        size=num_exporters,
                        p=exporter_probs,
                        replace=False
                    )
                    
                    exp_volumes = np.random.dirichlet(np.ones(num_exporters)) * volume
                    exp_volumes = np.round(exp_volumes).astype(int)
                    exp_volumes[-1] = volume - exp_volumes[:-1].sum()
                    
                    for exp, exp_volume in zip(chosen_exporters, exp_volumes):
                        if exp_volume <= 0:
                            continue
                            
                        sold_to_eu = False
                        if remaining_eu_exports > 0 and np.random.random() < exp.eu_preference:
                            sold_to_eu = True
                            remaining_eu_exports -= exp_volume
                        
                        new_relationships.append({
                            'year': year,
                            'country': country.id,
                            'farmer_id': farmer.id,
                            'middleman_id': m_id,
                            'exporter_id': exp.id,
                            'sold_to_eu': sold_to_eu,
                            'amount_kg': int(exp_volume)
                        })
    
    return new_relationships