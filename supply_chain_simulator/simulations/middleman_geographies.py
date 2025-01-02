from typing import List, Dict
import numpy as np
import ast
from collections import defaultdict
from scipy.spatial.distance import cdist

from models.geography import Geography
from models.actors import Middleman


def assign_middlemen_to_geographies(
    geographies: List[Geography],
    middlemen: List[Middleman],
    random_seed: int = 24
) -> Dict[str, List[Middleman]]:
    """
    Assign middlemen to geographies based on producing areas and competitiveness.
    Ensures every geography has at least two middlemen.
    """
    np.random.seed(random_seed)

    # Group geographies by producing area
    producing_areas = defaultdict(list)
    for geo in geographies:
        producing_areas[geo.producing_area_name].append(geo)
    
    # Calculate area weights based on number of farmers
    area_weights = {}
    for area, geos in producing_areas.items():
        area_weights[area] = sum(geo.num_farmers for geo in geos)
    
    # Validate we have enough middlemen for minimum coverage
    min_middlemen_needed = len(producing_areas) * 4  # 4 per producing area
    if len(middlemen) < min_middlemen_needed:
        raise ValueError(
            f"Not enough middlemen ({len(middlemen)}) to ensure minimum coverage "
            f"of 4 per producing area ({min_middlemen_needed} needed)"
        )

    # Distribute middlemen to producing areas
    area_middlemen = defaultdict(list)
    remaining_middlemen = middlemen.copy()

    # Ensure minimum 4 middlemen per producing area
    for area in producing_areas.keys():
        if len(remaining_middlemen) < 4:
            raise ValueError(f"Not enough remaining middlemen to assign to area {area}")
            
        selected = np.random.choice(
            remaining_middlemen,
            size=min(4, len(remaining_middlemen)),
            replace=False
        )
        area_middlemen[area].extend(selected)
        for m in selected:
            remaining_middlemen.remove(m)

    # Distribute remaining middlemen by farmer weights
    if remaining_middlemen:  # Only if we have middlemen left
        total_farmers = sum(area_weights.values())
        if total_farmers > 0:  # Prevent division by zero
            area_probabilities = {
                area: farmers / total_farmers
                for area, farmers in area_weights.items()
            }
            
            for middleman in remaining_middlemen:
                area = np.random.choice(
                    list(producing_areas.keys()),
                    p=[area_probabilities[area] for area in producing_areas.keys()]
                )
                area_middlemen[area].append(middleman)

    # Initialize geo_to_middlemen with empty lists
    geo_to_middlemen = {geo.id: [] for geo in geographies}
    
    # Assign middlemen to geographies within producing areas
    for area, geos in producing_areas.items():
        area_mids = area_middlemen[area]
        
        for middleman in area_mids:
            # Determine the number of geographies to assign
            coverage_pct = middleman.competitiveness
            num_geos = max(1, int(len(geos) * coverage_pct))
            
            if len(geos) > 0:  # Prevent division by zero
                # Calculate weights based on number of farmers
                geo_weights = [geo.num_farmers for geo in geos]
                geo_probs = np.array(geo_weights) / sum(geo_weights)
                
                # Select geographies for this middleman
                selected_geos = np.random.choice(
                    geos,
                    size=min(num_geos, len(geos)),
                    replace=False,
                    p=geo_probs
                )
                
                for geo in selected_geos:
                    geo_to_middlemen[geo.id].append(middleman)

    # Ensure minimum coverage (2 middlemen per geography)
    for geo_id, assigned_mms in geo_to_middlemen.items():
        if len(assigned_mms) < 2:
            geo = next(g for g in geographies if g.id == geo_id)
            area = geo.producing_area_name
            area_mids = area_middlemen[area]
            
            # Find unassigned middlemen for this geography
            unassigned = [m for m in area_mids if m not in assigned_mms]
            
            # Add middlemen until we have at least 2
            while len(assigned_mms) < 2 and unassigned:
                # Add the most competitive unassigned middleman
                next_mm = max(unassigned, key=lambda m: m.competitiveness)
                assigned_mms.append(next_mm)
                unassigned.remove(next_mm)

    return dict(geo_to_middlemen)