import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from pathlib import Path
import logging

from config.config import DB_CONFIG
from .schemas import SCHEMA_DEFINITIONS, PARTITION_STATEMENTS, INDEX_STATEMENTS

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, connection_params):
        self.connection_params = connection_params
        self._conn = None
        
    def initialize_database(self):
        """Initialize the database with required tables."""
        conn = self.get_connection()
        with conn.cursor() as cur:
            for table_sql in SCHEMA_DEFINITIONS.values():
                cur.execute(table_sql)
            conn.commit()
    
    def get_connection(self):
        """Get a database connection."""
        if not self._conn:
            self._conn = psycopg2.connect(**self.connection_params)
        return self._conn
    
    def commit(self):
        """Commit the current transaction."""
        if self._conn:
            self._conn.commit()
    
    def rollback(self):
        """Rollback the current transaction."""
        if self._conn:
            self._conn.rollback()
    
    def close(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def execute(self, query: str, params: tuple = ()) -> None:
        """Execute a query without returning results."""
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            conn.commit()
    
    def execute_many(self, query: str, params: List[tuple]) -> None:
        """Execute many queries in a single transaction."""
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.executemany(query, params)
            conn.commit()
    
    def fetch_one(self, query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """Fetch a single row from the database."""
        conn = self.get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchone()
    
    def fetch_all(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Fetch all rows from the database."""
        conn = self.get_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            result = cur.fetchall()
            return [dict(row) for row in result]
    
    def wipe_database(self) -> None:
        """Drop all tables in the database."""
        conn = self.get_connection()
        with conn.cursor() as cur:
            # Get all table names
            cur.execute("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname = 'public'
            """)
            tables = cur.fetchall()
            
            # Drop each table
            for table in tables:
                cur.execute(f'DROP TABLE IF EXISTS {table[0]} CASCADE')
            
            conn.commit()
            
        # Reinitialize the database with empty tables
        self.initialize_database()
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        try:
            yield
            self.commit()
        except Exception:
            self.rollback()
            raise
    
    @contextmanager
    def batch_operation(self):
        """Context manager for batch database operations."""
        try:
            yield self
            self.commit()
        except Exception as e:
            self.rollback()
            logger.error(f"Database error: {str(e)}")
            raise
    
    def execute_batch(self, query: str, params: List[tuple]):
        """Execute multiple operations in a single transaction."""
        with self.batch_operation():
            with self.get_connection().cursor() as cur:
                cur.executemany(query, params)
                
    def fetch_by_country(self, query: str, country_id: str) -> List[Dict]:
        """Common operation to fetch records by country."""
        return self.fetch_all(query, (country_id,))
    
    def create_partitions(self, country_id: str, year: int = None):
        """Create partitions for a country and optionally a specific year."""
        with self.transaction():
            # Create country-based partitions
            for table in ['geographies', 'farmers', 'middlemen', 'exporters']:
                self.execute(
                    PARTITION_STATEMENTS['create_country_partition'].format(
                        table_name=table,
                        country_id=country_id
                    )
                )
            
            # Create trading flow partitions if year specified
            if year is not None:
                # Create country partition for trading flows if it doesn't exist
                self.execute(
                    PARTITION_STATEMENTS['create_trading_partition'].format(
                        country_id=country_id,
                        year=year
                    )
                )
                
                # Create year partition
                self.execute(
                    PARTITION_STATEMENTS['create_year_partition'].format(
                        country_id=country_id,
                        year=year,
                        next_year=year + 1
                    )
                )
    
    def create_indexes(self, country_id: str, year: int = None):
        """Create indexes for partitioned tables."""
        with self.transaction():
            # Create indexes for country partitions
            for table in ['farmers', 'middlemen', 'exporters']:
                self.execute(
                    INDEX_STATEMENTS['create_geography_indexes'].format(
                        table=table,
                        country_id=country_id
                    )
                )
            
            # Create indexes for trading flows if year specified
            if year is not None:
                self.execute(
                    INDEX_STATEMENTS['create_trading_indexes'].format(
                        country_id=country_id,
                        year=year
                    )
                )