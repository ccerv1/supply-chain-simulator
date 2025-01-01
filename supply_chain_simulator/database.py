from typing import List
import sqlite3
from models import Country, Geography, Farmer, Middleman, Exporter

class DatabaseManager:
    CREATE_COUNTRIES_TABLE = """
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
    """

    CREATE_GEOGRAPHIES_TABLE = """
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
    """

    CREATE_EXPORTERS_TABLE = """
        CREATE TABLE IF NOT EXISTS exporters (
            id TEXT PRIMARY KEY,
            competitiveness REAL,
            eu_preference REAL,
            middleman_loyalty REAL
        )
    """

    CREATE_MIDDLEMEN_TABLE = """
        CREATE TABLE IF NOT EXISTS middlemen (
            id TEXT PRIMARY KEY,
            competitiveness REAL,
            farmer_loyalty REAL,
            exporter_loyalty REAL
        )
    """

    CREATE_FARMERS_TABLE = """
        CREATE TABLE IF NOT EXISTS farmers (
            id TEXT PRIMARY KEY,
            geography_id TEXT,
            num_plots INTEGER,
            production_amount REAL,
            middleman_loyalty REAL,
            FOREIGN KEY (geography_id) REFERENCES geographies (id)
        )
    """

    CREATE_TRADING_TABLE = """
        CREATE TABLE IF NOT EXISTS trading_relationships (
            year INTEGER,
            country TEXT,
            farmer_id TEXT,
            middleman_id TEXT,
            exporter_id TEXT,
            sold_to_eu BOOLEAN,
            amount_kg INTEGER,
            FOREIGN KEY (farmer_id) REFERENCES farmers (id),
            FOREIGN KEY (middleman_id) REFERENCES middlemen (id),
            FOREIGN KEY (exporter_id) REFERENCES exporters (id)
        )
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def __enter__(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            if exc_type is None:
                self.conn.commit()
            self.conn.close()

    def initialize_tables(self) -> None:
        """Create all necessary database tables."""
        self.cursor.execute(self.CREATE_COUNTRIES_TABLE)
        self.cursor.execute(self.CREATE_GEOGRAPHIES_TABLE)
        self.cursor.execute(self.CREATE_EXPORTERS_TABLE)
        self.cursor.execute(self.CREATE_MIDDLEMEN_TABLE)
        self.cursor.execute(self.CREATE_FARMERS_TABLE)
        self.cursor.execute(self.CREATE_TRADING_TABLE)

    def insert_country(self, country: Country) -> None:
        """Insert country data into database."""
        self.cursor.execute("""
            INSERT OR REPLACE INTO countries
            (id, name, total_production, num_farmers, num_middlemen, num_exporters,
             max_buyers_per_farmer, max_exporters_per_middleman, farmer_production_sigma,
             middleman_capacity_sigma, exporter_pareto_alpha, farmer_switch_rate,
             middleman_switch_rate, exports_to_eu, traceability_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            country.id,
            country.name,
            country.total_production,
            country.num_farmers,
            country.num_middlemen,
            country.num_exporters,
            country.max_buyers_per_farmer,
            country.max_exporters_per_middleman,
            country.farmer_production_sigma,
            country.middleman_capacity_sigma,
            country.exporter_pareto_alpha,
            country.farmer_switch_rate,
            country.middleman_switch_rate,
            country.exports_to_eu,
            country.traceability_rate
        ))

    def insert_geographies(self, geographies: List[Geography]) -> None:
        """Insert multiple geography records in a single transaction."""
        self.cursor.executemany("""
            INSERT OR REPLACE INTO geographies 
            (id, name, country_id, centroid, producing_area_name, num_farmers, 
            total_production_kg, primary_crop)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [(
            geo.id,
            geo.name,
            geo.country,
            str(geo.centroid),
            geo.producing_area_name,
            geo.num_farmers,
            geo.total_production_kg,
            geo.primary_crop
        ) for geo in geographies])

    def insert_farmers(self, farmers: List[Farmer]) -> None:
        """Insert multiple farmer records in a single transaction."""
        self.cursor.executemany("""
            INSERT OR REPLACE INTO farmers
            (id, geography_id, num_plots, production_amount, middleman_loyalty)
            VALUES (?, ?, ?, ?, ?)
        """, [(
            farmer.id,
            farmer.geography.id,
            int(farmer.num_plots),
            farmer.production_amount,
            farmer.middleman_loyalty
        ) for farmer in farmers])

    def insert_middlemen(self, middlemen: List[Middleman]) -> None:
        """Insert multiple middleman records in a single transaction."""
        self.cursor.executemany("""
            INSERT OR REPLACE INTO middlemen
            (id, competitiveness, farmer_loyalty, exporter_loyalty)
            VALUES (?, ?, ?, ?)
        """, [(
            mm.id,
            mm.competitiveness,
            mm.farmer_loyalty,
            mm.exporter_loyalty
        ) for mm in middlemen])

    def insert_exporters(self, exporters: List[Exporter]) -> None:
        """Insert multiple exporter records in a single transaction."""
        self.cursor.executemany("""
            INSERT OR REPLACE INTO exporters
            (id, competitiveness, eu_preference, middleman_loyalty)
            VALUES (?, ?, ?, ?)
        """, [(
            exp.id,
            exp.competitiveness,
            exp.eu_preference,
            exp.middleman_loyalty
        ) for exp in exporters])

    def insert_trading_relationships(self, relationships: List[dict]) -> None:
        """Insert multiple trading relationship records."""
        self.cursor.executemany("""
            INSERT INTO trading_relationships
            (year, country, farmer_id, middleman_id, exporter_id, sold_to_eu, amount_kg)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [(
            rel['year'],
            rel['country'],
            rel['farmer_id'],
            rel['middleman_id'],
            rel['exporter_id'],
            rel['sold_to_eu'],
            rel['amount_kg']
        ) for rel in relationships])