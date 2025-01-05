import logging
from typing import List, Dict, Any, Optional
from models.actors import Farmer, Middleman, Exporter
from models.geography import Country, Geography
from supply_chain_simulator.models.trade_flow import TradeFlow

logger = logging.getLogger(__name__)

class BaseRegistry:
    """Base class for all registries providing common database operations."""
    
    def __init__(self, db_manager):
        self.db = db_manager

    def get_by_country(self, country_id: str) -> List[Dict]:
        """Get records by country."""
        return self.db.fetch_all(
            f"SELECT * FROM {self.TABLE_NAME} WHERE country_id = %s",
            (country_id,)
        )

class CountryRegistry(BaseRegistry):
    """Registry for managing Country records."""
    
    def create(self, country: Country) -> None:
        try:
            self.db.execute("""
                INSERT INTO countries (
                    id, name, total_production, num_farmers,
                    num_middlemen, num_exporters, max_buyers_per_farmer,
                    max_exporters_per_middleman, farmer_production_sigma,
                    middleman_capacity_sigma, exporter_pareto_alpha,
                    farmer_switch_rate, middleman_switch_rate,
                    exports_to_eu, traceability_rate
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                country.id, country.name, country.total_production,
                country.num_farmers, country.num_middlemen, country.num_exporters,
                country.max_buyers_per_farmer, country.max_exporters_per_middleman,
                country.farmer_production_sigma, country.middleman_capacity_sigma,
                country.exporter_pareto_alpha, country.farmer_switch_rate,
                country.middleman_switch_rate, country.exports_to_eu,
                country.traceability_rate
            ))
            self.db.commit()  # Explicitly commit after creating country
        except Exception as e:
            self.db.rollback()
            raise
    
    def get_by_id(self, country_id: str) -> Optional[Country]:
        data = self.db.fetch_one(
            "SELECT * FROM countries WHERE id = %s",
            (country_id,)
        )
        return Country.from_dict(data) if data else None
    
    def get_all(self) -> List[Country]:
        data = self.db.fetch_all("SELECT * FROM countries")
        return [Country.from_dict(row) for row in data]


class GeographyRegistry(BaseRegistry):
    """Registry for managing Geography records."""
    
    def create_many(self, geographies: List[Geography]) -> None:
        try:
            self.db.execute_many("""
                INSERT INTO geographies (
                    id, name, country_id, centroid, producing_area_name,
                    num_farmers, total_production_kg, primary_crop
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, [
                (
                    g.id,
                    g.name,
                    g.country_id,
                    g.centroid,
                    g.producing_area_name,
                    g.num_farmers,
                    g.total_production_kg,
                    g.primary_crop
                ) for g in geographies
            ])
            self.db.commit()  # Ensure commit
        except Exception as e:
            self.db.rollback()
            raise
    
    def get_by_country(self, country_id: str) -> List[Geography]:
        data = self.db.fetch_all(
            "SELECT * FROM geographies WHERE country_id = %s",
            (country_id,)
        )
        return [Geography.from_dict(row) for row in data]
    
    def get_production_stats(self, country_id: str) -> Dict[str, int]:
        return self.db.fetch_one("""
            SELECT 
                SUM(num_farmers) as total_farmers,
                SUM(total_production_kg) as total_production
            FROM geographies 
            WHERE country_id = %s
        """, (country_id,))


class FarmerRegistry(BaseRegistry):
    """Registry for managing Farmer records."""
    
    def create_many(self, farmers: List[Farmer]) -> None:
        try:
            self.db.execute_many("""
                INSERT INTO farmers (
                    id, geography_id, country_id, num_plots,
                    production_amount, loyalty
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, [
                (
                    f.id,
                    f.geography_id,
                    f.country_id,
                    f.num_plots,
                    f.production_amount,
                    f.loyalty
                ) for f in farmers
            ])
            self.db.commit()  # Ensure commit
        except Exception as e:
            self.db.rollback()
            raise
    
    def get_by_geography(self, geography_id: str) -> List[Farmer]:
        data = self.db.fetch_all(
            "SELECT * FROM farmers WHERE geography_id = %s",
            (geography_id,)
        )
        return [Farmer.from_dict(row) for row in data]
    
    def get_all_by_country(self, country_id: str) -> List[Farmer]:
        data = self.db.fetch_all("""
            SELECT f.* 
            FROM farmers f
            JOIN geographies g ON f.geography_id = g.id
            WHERE g.country_id = %s
        """, (country_id,))
        return [Farmer.from_dict(row) for row in data]


class MiddlemanRegistry(BaseRegistry):
    """Registry for managing Middleman records."""
    
    def create_many(self, middlemen: List[Middleman]) -> None:
        try:
            self.db.execute_many("""
                INSERT INTO middlemen (
                    id, country_id, competitiveness, loyalty
                ) VALUES (%s, %s, %s, %s)
            """, [
                (
                    m.id,
                    m.country_id,
                    m.competitiveness,
                    m.loyalty
                ) for m in middlemen
            ])
            self.db.commit()  # Ensure commit
        except Exception as e:
            self.db.rollback()
            raise
    
    def get_all(self) -> List[Middleman]:
        data = self.db.fetch_all("SELECT * FROM middlemen")
        return [Middleman.from_dict(row) for row in data]
    
    def get_by_country(self, country_id: str) -> List[Middleman]:
        data = self.db.fetch_all("SELECT * FROM middlemen WHERE country_id = %s", (country_id,))
        return [Middleman.from_dict(row) for row in data]   


class ExporterRegistry(BaseRegistry):
    """Registry for managing Exporter records."""
    
    def create_many(self, exporters: List[Exporter]) -> None:
        try:
            self.db.execute_many("""
                INSERT INTO exporters (
                    id, country_id, competitiveness,
                    eu_preference, loyalty
                ) VALUES (%s, %s, %s, %s, %s)
            """, [
                (
                    e.id,
                    e.country_id,
                    e.competitiveness,
                    e.eu_preference,
                    e.loyalty
                ) for e in exporters
            ])
            self.db.commit()  # Ensure commit
        except Exception as e:
            self.db.rollback()
            raise
    
    def get_all(self) -> List[Exporter]:
        data = self.db.fetch_all("SELECT * FROM exporters")
        return [Exporter.from_dict(row) for row in data]

    def get_by_country(self, country_id: str) -> List[Exporter]:
        data = self.db.fetch_all("SELECT * FROM exporters WHERE country_id = %s", (country_id,))
        return [Exporter.from_dict(row) for row in data]


class TradingRegistry(BaseRegistry):
    """Registry for managing trading relationships."""
    
    def create_many(self, relationships: List[TradeFlow]) -> None:
        """Create trade flows, combining duplicates."""
        try:
            # Create a dictionary to combine duplicate flows
            combined_flows = {}
            
            for flow in relationships:
                key = (flow.year, flow.country_id, flow.farmer_id, 
                      flow.middleman_id, flow.exporter_id)
                if key in combined_flows:
                    # Add volumes for same flow
                    combined_flows[key].amount_kg += flow.amount_kg
                else:
                    combined_flows[key] = flow
            
            # Convert back to list and prepare for insert
            unique_flows = list(combined_flows.values())
            
            # Insert unique flows
            self.db.execute_many("""
                INSERT INTO trading_flows (
                    year, country_id, farmer_id, middleman_id,
                    exporter_id, sold_to_eu, amount_kg
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (country_id, year, farmer_id, middleman_id, exporter_id)
                DO UPDATE SET amount_kg = trading_flows.amount_kg + EXCLUDED.amount_kg
            """, [
                (
                    flow.year,
                    flow.country_id,
                    flow.farmer_id,
                    flow.middleman_id,
                    flow.exporter_id,
                    flow.sold_to_eu,
                    flow.amount_kg
                ) for flow in unique_flows
            ])
            
        except Exception as e:
            logger.error(f"Error creating trade flows: {str(e)}")
            raise
    
    def get_by_year(self, year: int) -> List[TradeFlow]:
        data = self.db.fetch_all(
            "SELECT * FROM trading_flows WHERE year = %s",
            (year,)
        )
        return [TradeFlow.from_dict(row) for row in data]
    
    def get_by_year_and_country(self, year: int, country_id: str) -> List[Dict]:
        """Get trading flows for a specific year and country."""
        return self.db.fetch_all("""
            SELECT * FROM trading_flows 
            WHERE year = %s AND country_id = %s
        """, (year, country_id))
    
    def get_by_year_and_middleman(self, year: int, middleman_id: str) -> List[TradeFlow]:
        data = self.db.fetch_all("""
            SELECT * FROM trading_flows 
            WHERE year = %s AND middleman_id = %s
        """, (year, middleman_id))
        return [TradeFlow.from_dict(row) for row in data]
    
    def get_year_summary(self, year: int, country_id: str) -> Dict[str, Any]:
        return self.db.fetch_one("""
            SELECT 
                COUNT(DISTINCT farmer_id) as num_farmers,
                COUNT(DISTINCT middleman_id) as num_middlemen,
                COUNT(DISTINCT exporter_id) as num_exporters,
                SUM(amount_kg) as total_volume,
                SUM(CASE WHEN sold_to_eu THEN amount_kg ELSE 0 END) as eu_volume,
                AVG(CASE WHEN sold_to_eu THEN 1.0 ELSE 0.0 END) as eu_ratio
            FROM trading_flows
            WHERE year = %s AND country_id = %s
        """, (year, country_id))
    
    def get_relationship_stats(self, year: int, country_id: str) -> Dict[str, Any]:
        return self.db.fetch_all("""
            WITH FarmerStats AS (
                SELECT 
                    farmer_id,
                    COUNT(DISTINCT middleman_id) as num_middlemen
                FROM trading_flows
                WHERE year = %s AND country_id = %s
                GROUP BY farmer_id
            ),
            MiddlemanStats AS (
                SELECT 
                    middleman_id,
                    COUNT(DISTINCT exporter_id) as num_exporters
                FROM trading_flows
                WHERE year = %s AND country_id = %s
                GROUP BY middleman_id
            )
            SELECT 
                'Farmers' as actor_type,
                AVG(num_middlemen) as avg_relationships,
                MIN(num_middlemen) as min_relationships,
                MAX(num_middlemen) as max_relationships
            FROM FarmerStats
            UNION ALL
            SELECT 
                'Middlemen' as actor_type,
                AVG(num_exporters) as avg_relationships,
                MIN(num_exporters) as min_relationships,
                MAX(num_exporters) as max_relationships
            FROM MiddlemanStats
        """, (year, country_id, year, country_id))
    
    def get_all_years(self) -> List[int]:
        """Get all years present in the trading relationships."""
        with self.db.get_connection() as conn:
            data = self.db.fetch_all("""
                SELECT DISTINCT year 
                FROM trading_flows 
                ORDER BY year
            """)
            return [row['year'] for row in data]
    
    def get_middleman_geographies(self, year: int, country_id: str, middleman_id: str) -> List[str]:
        """Get list of geography IDs that a middleman operated in for a given year."""
        data = self.db.fetch_all("""
            SELECT DISTINCT f.geography_id
            FROM trading_flows tr
            JOIN farmers f ON tr.farmer_id = f.id
            WHERE tr.year = %s 
            AND tr.country_id = %s
            AND tr.middleman_id = %s
        """, (year, country_id, middleman_id))
        return [row['geography_id'] for row in data]


class RelationshipRegistry(BaseRegistry):
    """Base class for relationship registries with common operations."""
    
    def create_many(self, relationships: List[Dict], year: int) -> None:
        """Create new relationships."""
        try:
            params = [
                (rel[self.FROM_ID_KEY], rel[self.TO_ID_KEY], year, None)
                for rel in relationships
            ]
            self.db.execute_many(self.INSERT_QUERY, params)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise

    def end_relationships(self, relationships: List[tuple], year: int) -> None:
        """Mark relationships as ended in the given year."""
        try:
            for from_id, to_id in relationships:
                self.db.execute(self.UPDATE_QUERY, (year, from_id, to_id))
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise

    def get_active_relationships(self, year: int, country_id: str) -> List[Dict]:
        """Get active relationships for a specific year."""
        return self.db.fetch_all(f"""
            SELECT {self.FROM_ID_KEY}, {self.TO_ID_KEY}
            FROM {self.TABLE_NAME}
            WHERE created_at <= %s 
            AND (deleted_at IS NULL OR deleted_at > %s)
        """, (year, year))


class MiddlemanGeographyRegistry(RelationshipRegistry):
    """Registry for middleman-geography relationships."""
    TABLE_NAME = 'middleman_geography_relationships'
    
    INSERT_QUERY = """
        INSERT INTO middleman_geography_relationships 
        (middleman_id, geography_id, created_at, deleted_at)
        VALUES (%s, %s, %s, %s)
    """
    
    UPDATE_QUERY = """
        UPDATE middleman_geography_relationships
        SET deleted_at = %s
        WHERE middleman_id = %s 
        AND geography_id = %s
        AND deleted_at IS NULL
    """

    FROM_ID_KEY = 'middleman_id'
    TO_ID_KEY = 'geography_id'


class FarmerMiddlemanRegistry(RelationshipRegistry):
    """Registry for farmer-middleman relationships."""
    TABLE_NAME = 'farmer_middleman_relationships'
    
    INSERT_QUERY = """
        INSERT INTO farmer_middleman_relationships 
        (farmer_id, middleman_id, created_at, deleted_at)
        VALUES (%s, %s, %s, %s)
    """
    
    UPDATE_QUERY = """
        UPDATE farmer_middleman_relationships
        SET deleted_at = %s
        WHERE farmer_id = %s 
        AND middleman_id = %s
        AND deleted_at IS NULL
    """

    FROM_ID_KEY = 'farmer_id'
    TO_ID_KEY = 'middleman_id'


class MiddlemanExporterRegistry(RelationshipRegistry):
    """Registry for middleman-exporter relationships."""
    TABLE_NAME = 'middleman_exporter_relationships'
    
    INSERT_QUERY = """
        INSERT INTO middleman_exporter_relationships 
        (middleman_id, exporter_id, created_at, deleted_at)
        VALUES (%s, %s, %s, %s)
    """
    
    UPDATE_QUERY = """
        UPDATE middleman_exporter_relationships
        SET deleted_at = %s
        WHERE middleman_id = %s 
        AND exporter_id = %s
        AND deleted_at IS NULL
    """

    FROM_ID_KEY = 'middleman_id'
    TO_ID_KEY = 'exporter_id'
