
# Coffee Supply Chain Simulator: Technical Overview

## Overview

The Coffee Supply Chain Simulator is designed to model the intricate web of relationships and trade flows in coffee-producing regions. It offers a comprehensive representation of interactions between farmers, middlemen, and exporters, incorporating geographic and temporal dynamics. By leveraging a multi-year Monte Carlo simulation, the system captures the evolution of these relationships and provides insights into market behaviors. It also facilitates scenario testing, enabling users to explore the impacts of policy changes, market dynamics, and geographic factors on the coffee supply chain.

---

## Architecture

### Core Components

The simulator is organized into three main components:

1. **Database Layer** (`database/`):
  
  The database include a representation for over 10 million farmers, thousands of middlemen, and hundreds of exporters. It also includes detailed geographic data for over 20 countries where Enveritas operates.

  The database captures the many-to-many relationships between farmers and middlemen, and middlemen and exporters. Thus, for any given simulation year, there can easily be over 100 million relationships between farmers and middlemen (eg, 10 million farmers x 3 middlemen per farmer x 3 exporters per middlemen x 2 possible destinations).  

  Currently, the database requires a local installation of PostgreSQL and runs on a single machine.

2. **Simulation Engine** (`simulations/`):
  
  This is the heart of the application, responsible for generating and evolving trade networks. It includes modules for initializing all actors in a country, geographic assignments, relationship management, trade flow generation, and year-over-year relationship evolution. The engine integrates geographic constraints to ensure realistic simulation outcomes.

   It can take several hours to run 2-3 years of simulations for all countries.

3. **Model Definitions** (`models/`):
  
  This component defines the key entities in the supply chain: farmers, middlemen, and exporters. It also includes models for countries and geographies.

  The model assumes that `production` and `num_plots` is fixed for each farmer. The production is split among middlemen and exporters based on their `competitiveness` score. Over time, the willingness for an actor to continue a relationship is based on their `loyalty` score. These features are randomized once at the start of the simulation. Exporters also have a fixed `eu_preference` score.

  Country-level data is also included in the model, including EU trade targets and specific randomization parameters. The geographic data includes both a `geographic_id` and a larger production area that is used to determine the number of middlemen in the area. We include centroid coordinates for each geographic area but currently do not use them in the simulation. As result, middlemen may not always buy from contiguous geographic areas.

---

## Methodology

### 1. Geographic Network Initialization

#### Producing Area Assignment

To establish geographic coverage, the simulation employs a hierarchical approach. Each producing area is assigned a minimum of four middlemen, and every broader geographic region is guaranteed at least two middlemen. The assignment process accounts for the farmer population, weighting the distribution proportionally:

```python
P(middleman_to_area) = N_farmers_in_area / N_total_farmers
```

This ensures reasonable representation across regions while adhering to minimum assignment constraints. The assignment process also scales based on the competitiveness of middlemen:

```python
num_geographies = max(1, int(len(geos) * middleman.competitiveness))
P(middleman_to_geography) = N_farmers_in_geo / N_farmers_in_area
```

In other words, a more competitive middlemen will often be assigned to more geographies.

Overall, the system tries to ensure that middlemen operate across multiple geographies, proportional to the density of farmers in each region. This approach balances geographic coverage and resource allocation.

---

### 2. Actor Initialization

#### Farmer Generation
Farmers are initialized with production volumes following a log-normal distribution, reflecting realistic production variations:

```python
μ = ln(mean_production)
σ = 0.5  # Configurable variance
production = exp(N(μ, σ))
```

Constraints are applied to align total production with national targets while maintaining regional proportionality.

#### Middleman and Exporter Creation
The number of middlemen and exporters per country are passed as parameters to the simulation engine. These can be updated in the `country_assums.csv` file.

All actors are initialized wiht a `loyalty` score between 0 and 1. This score is used to determine the likelihood of a relationship continuing from one year to the next.

Middleman and exporter actors are initialized with a `competitiveness` score between 0 and 1. This score is used to determine the number of relationships they will have with upstream actors.

---

### 3. Relationship Evolution

#### Initial Assignment (Year 0)
Initial relationships between farmers, middlemen, and exporters are established using probability distributions:

```python
P(farmer_to_middleman) = 1 / N_available_middlemen
P(middleman_to_exporter) = competitiveness_score * (1 / N_available_exporters)
```

#### Year-over-Year Changes
Relationships evolve based on loyalty scores, which increase with the duration of the relationship:

```python
P(relationship_continues) = base_loyalty + age_factor
age_factor = min(0.3, years_active * 0.1)
```

This models the gradual strengthening of long-term relationships while allowing for dynamic market adjustments.

---

### 4. Trade Flow Generation

#### Volume Distribution
Trade volumes are distributed using a Dirichlet distribution, ensuring a fair allocation of production across relationships:

```python
volumes = production * Dir(α=[1,...,1])  # α=1 for uniform distribution
```

#### EU Sales Determination
Export decisions are influenced by exporter preferences and national EU trade targets:

```python
P(sale_to_eu) = min(
    exporter.eu_preference,
    (eu_target / total_production) * (1 + 0.1 * (eu_preference - 0.5))
)
```

---

## Database Schema

### Core Tables

1. **`farmers`**:
   - Tracks production volumes, geographic locations, and loyalty scores.
   - Partitioned by country for efficient querying.

2. **`middlemen`**:
   - Contains competitiveness metrics and geographic coverage information.

3. **`exporters`**:
   - Stores EU preferences and market competitiveness metrics.

### Relationship Tables

1. **`middleman_geography_relationships`**:
   - Tracks temporal validity and geographic coverage.

2. **`farmer_middleman_relationships`**:
   - Captures trade relationships and volume allocations.

3. **`middleman_exporter_relationships`**:
   - Represents connections between middlemen and exporters.

### Trading Flows

1. **`trading_flows`**:
   - Tracks the flow of production from farmers to middlemen and then to exporters.

---

## Queries for Analyzing Sales to the EU

This query shows the number of unique farmers selling to EU vs non-EU destinations by country and year:
```
SELECT 
    country_id,
    year,
    COUNT(DISTINCT farmer_id) as num_farmers,
    SUM(amount_kg) as eu_sales
FROM trading_flows
WHERE sold_to_eu = True
GROUP BY country_id, year;
```

We can also see the total number of farmers selling to the EU over all three years:
```
SELECT
    COUNT(DISTINCT farmer_id) as num_farmers
FROM trading_flows
WHERE sold_to_eu = True;
```

Finally, we can look up the number of plots those farmers have:
```
WITH eu_sales AS (
    SELECT
        country_id,
        farmer_id,
        SUM(amount_kg) as eu_sales
    FROM trading_flows
    WHERE sold_to_eu = True
    GROUP BY country_id, farmer_id
)
SELECT
    es.country_id,
    COUNT(DISTINCT es.farmer_id) as num_farmers,
    SUM(f.num_plots) as num_plots
FROM eu_sales es
JOIN farmers f ON es.farmer_id = f.id
GROUP BY es.country_id;
```

---

## Model Validation

### Volume Conservation
Ensures total farmer production matches trade flows:

```sql
SELECT 
    t.country_id,
    SUM(f.production_amount) as total_production,
    SUM(t.amount_kg) as total_traded,
    ABS(SUM(f.production_amount) - SUM(t.amount_kg)) as volume_difference
FROM trading_flows t
JOIN farmers f ON t.farmer_id = f.id
GROUP BY t.country_id;
```

---

## Performance Considerations

The system is optimized for scalability through database partitioning, batch processing, and parallelization. Temporal indexing ensures efficient querying of historical data.

---

## Limitations and Assumptions

While the simulator provides a robust framework, it assumes fixed geographic boundaries and simplified market dynamics. Enhancing these aspects could improve model accuracy.

---

## Future Enhancements

Proposed improvements include:
- Analystics and visualization tools for exploring the simulation results.
- More efficient simulation engine for trading flows.
- Better geographic clustering of middlemen.
- More sophisticated relationship evolution model.