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
    
    # Calculate weights for each producing area based on total farmers
    area_weights = defaultdict(int)
    for geo in geographies:
        area_weights[geo.producing_area_name] += geo.num_farmers
    
    # Normalize weights
    total_farmers = sum(area_weights.values())
    area_probabilities = {
        area: farmers / total_farmers
        for area, farmers in area_weights.items()
    }

    # Distribute middlemen to producing areas
    area_middlemen = defaultdict(list)
    remaining_middlemen = middlemen.copy()

    # Ensure minimum 4 middlemen per producing area
    for area in producing_areas.keys():
        selected = np.random.choice(
            remaining_middlemen,
            size=4,
            replace=False
        )
        area_middlemen[area].extend(selected)
        for m in selected:
            remaining_middlemen.remove(m)

    # Distribute remaining middlemen by farmer weights
    for middleman in remaining_middlemen:
        area = np.random.choice(
            list(producing_areas.keys()),
            p=[area_probabilities[area] for area in producing_areas.keys()]
        )
        area_middlemen[area].append(middleman)

    # Assign middlemen to geographies within producing areas
    geo_to_middlemen = defaultdict(list)
    assigned_middlemen = []
    
    for area, geos in producing_areas.items():
        area_mids = area_middlemen[area]

        for middleman in area_mids:
            # Determine the number of geographies to assign based on competitiveness
            coverage_pct = middleman.competitiveness
            num_geos = max(1, int(len(geos) * coverage_pct))

            # Calculate weights based on number of farmers in each geography
            geo_weights = [geo.num_farmers for geo in geos]
            geo_probs = np.array(geo_weights) / sum(geo_weights)

            # Randomly select geographies for the middleman, weighted by farmer count
            selected_geos = np.random.choice(
                geos,
                size=min(num_geos, len(geos)),
                replace=False,
                p=geo_probs
            )

            for geo in selected_geos:
                geo_to_middlemen[geo.id].append(middleman)
                assigned_middlemen.append(middleman)

    # Ensure every geography has at least two middlemen
    for geo in geographies:
        assigned_middlemen = geo_to_middlemen[geo.id]
        if len(assigned_middlemen) < 2:
            needed = 2 - len(assigned_middlemen)
            area_middlemen_pool = area_middlemen[geo.producing_area_name]
            unassigned = [m for m in area_middlemen_pool if m not in assigned_middlemen]

            if unassigned:
                additional_middlemen = sorted(unassigned, key=lambda m: m.competitiveness, reverse=True)[:needed]
                geo_to_middlemen[geo.id].extend(additional_middlemen)
            else:
                most_competitive = sorted(area_middlemen_pool, key=lambda m: m.competitiveness, reverse=True)[:needed]
                geo_to_middlemen[geo.id].extend(most_competitive)

    return dict(geo_to_middlemen)