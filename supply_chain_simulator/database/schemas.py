"""SQL table definitions for the supply chain database"""

SCHEMA_DEFINITIONS = {
    'countries': """
        CREATE TABLE IF NOT EXISTS countries (
            id TEXT PRIMARY KEY,
            name TEXT,
            total_production INTEGER,
            num_farmers INTEGER,
            num_middlemen INTEGER,
            num_exporters INTEGER,
            max_buyers_per_farmer INTEGER,
            max_exporters_per_middleman INTEGER,
            farmer_production_sigma REAL,
            middleman_capacity_sigma REAL,
            exporter_pareto_alpha REAL,
            farmer_switch_rate REAL,
            middleman_switch_rate REAL,
            exports_to_eu INTEGER,
            traceability_rate REAL
        )
    """,
    
    'geographies': """
        CREATE TABLE IF NOT EXISTS geographies (
            id TEXT PRIMARY KEY,
            name TEXT,
            country_id TEXT,
            centroid TEXT,
            producing_area_name TEXT,
            num_farmers INTEGER,
            total_production_kg INTEGER,
            primary_crop TEXT,
            FOREIGN KEY (country_id) REFERENCES countries (id)
        )
    """,
    
    'farmers': """
        CREATE TABLE IF NOT EXISTS farmers (
            id TEXT PRIMARY KEY,
            geography_id TEXT,
            country_id TEXT,
            num_plots INTEGER,
            production_amount REAL,
            loyalty REAL,
            FOREIGN KEY (geography_id) REFERENCES geographies (id),
            FOREIGN KEY (country_id) REFERENCES countries (id)
        )
    """,
    
    'middlemen': """
        CREATE TABLE IF NOT EXISTS middlemen (
            id TEXT PRIMARY KEY,
            country_id TEXT,
            competitiveness REAL,
            loyalty REAL,
            FOREIGN KEY (country_id) REFERENCES countries (id)
        )
    """,
    
    'exporters': """
        CREATE TABLE IF NOT EXISTS exporters (
            id TEXT PRIMARY KEY,
            country_id TEXT,
            competitiveness REAL,
            eu_preference REAL,
            loyalty REAL,
            FOREIGN KEY (country_id) REFERENCES countries (id)
        )
    """,
    
    'trading_flows': """
        CREATE TABLE IF NOT EXISTS trading_flows (
            year INTEGER,
            country_id TEXT,
            farmer_id TEXT,
            middleman_id TEXT,
            exporter_id TEXT,
            sold_to_eu BOOLEAN,
            amount_kg INTEGER,
            FOREIGN KEY (farmer_id) REFERENCES farmers (id),
            FOREIGN KEY (middleman_id) REFERENCES middlemen (id),
            FOREIGN KEY (exporter_id) REFERENCES exporters (id),
            FOREIGN KEY (country_id) REFERENCES countries (id)
        )
    """
}