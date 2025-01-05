"""SQL table definitions for the supply chain database"""

SCHEMA_DEFINITIONS = {
    'countries': """
        CREATE TABLE IF NOT EXISTS countries (
            id VARCHAR(2) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            total_production BIGINT NOT NULL,
            num_farmers BIGINT NOT NULL,
            num_middlemen INTEGER NOT NULL,
            num_exporters INTEGER NOT NULL,
            max_buyers_per_farmer INTEGER NOT NULL DEFAULT 3,
            max_exporters_per_middleman INTEGER NOT NULL DEFAULT 5,
            farmer_production_sigma FLOAT NOT NULL DEFAULT 0.5,
            middleman_capacity_sigma FLOAT NOT NULL DEFAULT 0.5,
            exporter_pareto_alpha FLOAT NOT NULL DEFAULT 2.0,
            farmer_switch_rate FLOAT NOT NULL DEFAULT 0.2,
            middleman_switch_rate FLOAT NOT NULL DEFAULT 0.2,
            exports_to_eu BIGINT NOT NULL,
            traceability_rate FLOAT NOT NULL DEFAULT 0.5
        )
    """,
    
    'geographies': """
        CREATE TABLE IF NOT EXISTS geographies (
            id TEXT,
            country_id TEXT,
            name TEXT,
            centroid TEXT,
            producing_area_name TEXT,
            num_farmers INTEGER,
            total_production_kg INTEGER,
            primary_crop TEXT,
            FOREIGN KEY (country_id) REFERENCES countries (id)
            PRIMARY KEY (id, country_id)
        ) PARTITION BY LIST (country_id)
    """,
    
    'farmers': """
        CREATE TABLE IF NOT EXISTS farmers (
            id TEXT,
            country_id TEXT,
            geography_id TEXT,
            num_plots INTEGER,
            production_amount REAL,
            loyalty REAL,
            FOREIGN KEY (geography_id) REFERENCES geographies (id),
            FOREIGN KEY (country_id) REFERENCES countries (id)
            PRIMARY KEY (id, country_id)
        ) PARTITION BY LIST (country_id)
    """,
    
    'middlemen': """
        CREATE TABLE IF NOT EXISTS middlemen (
            id TEXT,
            country_id TEXT,
            competitiveness REAL,
            loyalty REAL,
            FOREIGN KEY (country_id) REFERENCES countries (id)
            PRIMARY KEY (id, country_id)
        ) PARTITION BY LIST (country_id)
    """,
    
    'exporters': """
        CREATE TABLE IF NOT EXISTS exporters (
            id TEXT,
            country_id TEXT,
            competitiveness REAL,
            eu_preference REAL,
            loyalty REAL,
            FOREIGN KEY (country_id) REFERENCES countries (id)
            PRIMARY KEY (id, country_id)
        ) PARTITION BY LIST (country_id)
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
            PRIMARY KEY (country_id, year, farmer_id, middleman_id, exporter_id)
        ) PARTITION BY LIST (country_id), RANGE (year)
    """,
    
    'middleman_geography_relationships': """
        CREATE TABLE IF NOT EXISTS middleman_geography_relationships (
            id SERIAL PRIMARY KEY,
            middleman_id TEXT,
            geography_id TEXT,
            created_at INTEGER NOT NULL,  -- year created
            deleted_at INTEGER,           -- year relationship ended
            FOREIGN KEY (middleman_id) REFERENCES middlemen (id),
            FOREIGN KEY (geography_id) REFERENCES geographies (id)
        )
    """,
    
    'farmer_middleman_relationships': """
        CREATE TABLE IF NOT EXISTS farmer_middleman_relationships (
            id SERIAL PRIMARY KEY,
            farmer_id TEXT,
            middleman_id TEXT,
            created_at INTEGER NOT NULL,
            deleted_at INTEGER,
            FOREIGN KEY (farmer_id) REFERENCES farmers (id),
            FOREIGN KEY (middleman_id) REFERENCES middlemen (id)
        )
    """,
    
    'middleman_exporter_relationships': """
        CREATE TABLE IF NOT EXISTS middleman_exporter_relationships (
            id SERIAL PRIMARY KEY,
            middleman_id TEXT,
            exporter_id TEXT,
            created_at INTEGER NOT NULL,
            deleted_at INTEGER,
            FOREIGN KEY (middleman_id) REFERENCES middlemen (id),
            FOREIGN KEY (exporter_id) REFERENCES exporters (id)
        )
    """
}

# Add partition creation statements
PARTITION_STATEMENTS = {
    'create_country_partition': """
        CREATE TABLE IF NOT EXISTS {table_name}_{country_id} 
        PARTITION OF {table_name}
        FOR VALUES IN ('{country_id}')
    """,
    
    'create_trading_partition': """
        CREATE TABLE IF NOT EXISTS trading_flows_{country_id}_{year} 
        PARTITION OF trading_flows
        FOR VALUES IN ('{country_id}')
        PARTITION BY RANGE (year)
    """,
    
    'create_year_partition': """
        CREATE TABLE IF NOT EXISTS trading_flows_{country_id}_{year} 
        PARTITION OF trading_flows_{country_id}
        FOR VALUES FROM ({year}) TO ({next_year})
    """
}

INDEX_STATEMENTS = {
    'create_geography_indexes': """
        CREATE INDEX IF NOT EXISTS idx_{table}_{country_id}_geo 
        ON {table}_{country_id} (geography_id)
    """,
    
    'create_trading_indexes': """
        CREATE INDEX IF NOT EXISTS idx_trading_{country_id}_{year}_farmer 
        ON trading_flows_{country_id}_{year} (farmer_id);
        CREATE INDEX IF NOT EXISTS idx_trading_{country_id}_{year}_middleman 
        ON trading_flows_{country_id}_{year} (middleman_id);
        CREATE INDEX IF NOT EXISTS idx_trading_{country_id}_{year}_exporter 
        ON trading_flows_{country_id}_{year} (exporter_id);
    """
}