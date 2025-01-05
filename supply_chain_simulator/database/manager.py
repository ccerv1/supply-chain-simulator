import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
import logging

from config.config import DB_CONFIG
from .schemas import SCHEMA_DEFINITIONS

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, connection_params):
        self.connection_params = connection_params
        self._conn = None
        
    def initialize_database(self):
        """Initialize the database with required tables."""
        with self.transaction():
            try:
                # Create base tables first
                for table_name, table_sql in SCHEMA_DEFINITIONS.items():
                    logger.debug(f"Creating table: {table_name}")
                    self.execute(table_sql)
                logger.info("Base tables created successfully")
            except Exception as e:
                logger.error(f"Error creating tables: {str(e)}")
                raise
    
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
            if params:
                cur.execute(query, params)
            else:
                # For DDL statements and raw SQL
                cur.execute(query)
            conn.commit()
    
    def execute_ddl(self, query: str) -> None:
        """Execute DDL statements (CREATE, DROP, etc.)."""
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.execute(query)
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
        """Drop all tables and recreate them."""
        with self.transaction():
            try:
                # Drop all tables in reverse order to handle dependencies
                tables = [
                    'trading_flows', 'middleman_exporter_relationships',
                    'farmer_middleman_relationships', 'middleman_geography_relationships',
                    'exporters', 'middlemen', 'farmers', 'geographies', 'countries'
                ]
                
                for table in tables:
                    self.execute_ddl(f"DROP TABLE IF EXISTS {table} CASCADE")
                
                # Recreate tables
                self.initialize_database()
                
            except Exception as e:
                logger.error(f"Error wiping database: {str(e)}")
                raise
    
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
        """Execute multiple operations efficiently."""
        with self.batch_operation():
            with self.get_connection().cursor() as cur:
                # Use server-side cursor for large datasets
                cur.itersize = 2000
                cur.executemany(query, params)
                
    def fetch_by_country(self, query: str, country_id: str) -> List[Dict]:
        """Common operation to fetch records by country."""
        return self.fetch_all(query, (country_id,))