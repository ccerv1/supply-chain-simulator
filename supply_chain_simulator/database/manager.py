import sqlite3
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
from pathlib import Path

from .schemas import SCHEMA_DEFINITIONS

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.initialize_database()
    
    def initialize_database(self) -> None:
        """Create database tables if they don't exist."""
        with self.get_connection() as conn:
            for table_sql in SCHEMA_DEFINITIONS.values():
                conn.execute(table_sql)
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def execute(self, query: str, params: tuple = ()) -> None:
        """Execute a query without returning results."""
        with self.get_connection() as conn:
            conn.execute(query, params)
    
    def execute_many(self, query: str, params: List[tuple]) -> None:
        """Execute many queries in a single transaction."""
        with self.get_connection() as conn:
            conn.executemany(query, params)
    
    def fetch_one(self, query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """Fetch a single row from the database."""
        with self.get_connection() as conn:
            result = conn.execute(query, params).fetchone()
            return dict(result) if result else None
    
    def fetch_all(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """Fetch all rows from the database."""
        with self.get_connection() as conn:
            results = conn.execute(query, params).fetchall()
            return [dict(row) for row in results]
    
    def wipe_database(self) -> None:
        """Drop all tables in the database."""
        with self.get_connection() as conn:
            # Disable foreign key checks temporarily to avoid dependency issues
            conn.execute("PRAGMA foreign_keys = OFF")
            
            # Get all table names
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            
            # Drop each table
            for table in tables:
                conn.execute(f"DROP TABLE IF EXISTS {table[0]}")
            
            # Re-enable foreign key checks
            conn.execute("PRAGMA foreign_keys = ON")
            
            # Reinitialize the database with empty tables
            self.initialize_database()