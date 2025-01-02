from typing import List, Optional, Dict, Any
from models.actors import Farmer, Middleman, Exporter
from models.geography import Country, Geography
from models.relationships import TradingRelationship

class BaseRegistry:
    """Base class for all registries providing common database operations."""
    
    def __init__(self, db_manager):
        self.db = db_manager


class CountryRegistry(BaseRegistry):
    """Registry for managing Country records."""
    
    def create(self, country: Country) -> None:
        self.db.execute("""
            INSERT INTO countries VALUES (
                :id, :name, :total_production, :num_farmers, :num_middlemen,
                :num_exporters, :max_buyers_per_farmer, :max_exporters_per_middleman,
                :farmer_production_sigma, :middleman_capacity_sigma,
                :exporter_pareto_alpha, :farmer_switch_rate, :middleman_switch_rate,
                :exports_to_eu, :traceability_rate
            )
        """, country.to_dict())
    
    def get_by_id(self, country_id: str) -> Optional[Country]:
        data = self.db.fetch_one(
            "SELECT * FROM countries WHERE id = ?", 
            (country_id,)
        )
        return Country.from_dict(data) if data else None
    
    def get_all(self) -> List[Country]:
        data = self.db.fetch_all("SELECT * FROM countries")
        return [Country.from_dict(row) for row in data]


class GeographyRegistry(BaseRegistry):
    """Registry for managing Geography records."""
    
    def create_many(self, geographies: List[Geography]) -> None:
        self.db.execute_many("""
            INSERT INTO geographies VALUES (
                :id, :name, :country_id, :centroid, :producing_area_name,
                :num_farmers, :total_production_kg, :primary_crop
            )
        """, [g.to_dict() for g in geographies])
    
    def get_by_country(self, country_id: str) -> List[Geography]:
        data = self.db.fetch_all(
            "SELECT * FROM geographies WHERE country_id = ?",
            (country_id,)
        )
        return [Geography.from_dict(row) for row in data]
    
    def get_production_stats(self, country_id: str) -> Dict[str, int]:
        return self.db.fetch_one("""
            SELECT 
                SUM(num_farmers) as total_farmers,
                SUM(total_production_kg) as total_production
            FROM geographies 
            WHERE country_id = ?
        """, (country_id,))


class FarmerRegistry(BaseRegistry):
    """Registry for managing Farmer records."""
    
    def create_many(self, farmers: List[Farmer]) -> None:
        self.db.execute_many("""
            INSERT INTO farmers VALUES (
                :id, :geography_id, :country_id, :num_plots, :production_amount, :loyalty
            )
        """, [f.to_dict() for f in farmers])
    
    def get_by_geography(self, geography_id: str) -> List[Farmer]:
        data = self.db.fetch_all(
            "SELECT * FROM farmers WHERE geography_id = ?",
            (geography_id,)
        )
        return [Farmer.from_dict(row) for row in data]
    
    def get_all_by_country(self, country_id: str) -> List[Farmer]:
        data = self.db.fetch_all("""
            SELECT f.* 
            FROM farmers f
            JOIN geographies g ON f.geography_id = g.id
            WHERE g.country_id = ?
        """, (country_id,))
        return [Farmer.from_dict(row) for row in data]


class MiddlemanRegistry(BaseRegistry):
    """Registry for managing Middleman records."""
    
    def create_many(self, middlemen: List[Middleman]) -> None:
        self.db.execute_many("""
            INSERT INTO middlemen VALUES (
                :id, :country_id, :competitiveness, :loyalty
            )
        """, [m.to_dict() for m in middlemen])
    
    def get_all(self) -> List[Middleman]:
        data = self.db.fetch_all("SELECT * FROM middlemen")
        return [Middleman.from_dict(row) for row in data]


class ExporterRegistry(BaseRegistry):
    """Registry for managing Exporter records."""
    
    def create_many(self, exporters: List[Exporter]) -> None:
        self.db.execute_many("""
            INSERT INTO exporters VALUES (
                :id, :country_id, :competitiveness, :eu_preference, :loyalty
            )
        """, [e.to_dict() for e in exporters])
    
    def get_all(self) -> List[Exporter]:
        data = self.db.fetch_all("SELECT * FROM exporters")
        return [Exporter.from_dict(row) for row in data]


class TradingRegistry(BaseRegistry):
    """Registry for managing trading relationships."""
    
    def create_many(self, relationships: List[TradingRelationship]) -> None:
        self.db.execute_many("""
            INSERT INTO trading_relationships VALUES (
                :year, :country_id, :farmer_id, :middleman_id, :exporter_id,
                :sold_to_eu, :amount_kg
            )
        """, [r.to_dict() for r in relationships])
    
    def get_by_year(self, year: int) -> List[TradingRelationship]:
        data = self.db.fetch_all(
            "SELECT * FROM trading_relationships WHERE year = ?",
            (year,)
        )
        return [TradingRelationship.from_dict(row) for row in data]
    
    def get_by_year_and_country(self, year: int, country_id: str) -> List[TradingRelationship]:
        data = self.db.fetch_all("""
            SELECT * FROM trading_relationships 
            WHERE year = ? AND country_id = ?
        """, (year, country_id))
        return [TradingRelationship.from_dict(row) for row in data]
    
    def get_by_year_and_middleman(self, year: int, middleman_id: str) -> List[TradingRelationship]:
        data = self.db.fetch_all("""
            SELECT * FROM trading_relationships 
            WHERE year = ? AND middleman_id = ?
        """, (year, middleman_id))
        return [TradingRelationship.from_dict(row) for row in data]
    
    def get_year_summary(self, year: int, country_id: str) -> Dict[str, Any]:
        return self.db.fetch_one("""
            SELECT 
                COUNT(DISTINCT farmer_id) as num_farmers,
                COUNT(DISTINCT middleman_id) as num_middlemen,
                COUNT(DISTINCT exporter_id) as num_exporters,
                SUM(amount_kg) as total_volume,
                SUM(CASE WHEN sold_to_eu THEN amount_kg ELSE 0 END) as eu_volume,
                AVG(CASE WHEN sold_to_eu THEN 1.0 ELSE 0.0 END) as eu_ratio
            FROM trading_relationships
            WHERE year = ? AND country_id = ?
        """, (year, country_id))
    
    def get_relationship_stats(self, year: int, country_id: str) -> Dict[str, Any]:
        return self.db.fetch_all("""
            WITH FarmerStats AS (
                SELECT 
                    farmer_id,
                    COUNT(DISTINCT middleman_id) as num_middlemen
                FROM trading_relationships
                WHERE year = ? AND country_id = ?
                GROUP BY farmer_id
            ),
            MiddlemanStats AS (
                SELECT 
                    middleman_id,
                    COUNT(DISTINCT exporter_id) as num_exporters
                FROM trading_relationships
                WHERE year = ? AND country_id = ?
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
        data = self.db.fetch_all("""
            SELECT DISTINCT year 
            FROM trading_relationships 
            ORDER BY year
        """)
        return [row['year'] for row in data]
    
    def get_middleman_geographies(self, year: int, country_id: str, middleman_id: str) -> List[str]:
        """Get list of geography IDs that a middleman operated in for a given year."""
        data = self.db.fetch_all("""
            SELECT DISTINCT f.geography_id
            FROM trading_relationships tr
            JOIN farmers f ON tr.farmer_id = f.id
            WHERE tr.year = ? 
            AND tr.country_id = ?
            AND tr.middleman_id = ?
        """, (year, country_id, middleman_id))
        return [row['geography_id'] for row in data]