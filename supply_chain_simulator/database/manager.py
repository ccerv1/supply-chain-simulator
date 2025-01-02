import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from pathlib import Path

from config.config import DB_CONFIG
from .schemas import SCHEMA_DEFINITIONS

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
    
    def execute_many(self, query: str, params: List[tuple]) -> None:
        """Execute many queries in a single transaction."""
        conn = self.get_connection()
        with conn.cursor() as cur:
            cur.executemany(query, params)
    
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
            return cur.fetchall()
    
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