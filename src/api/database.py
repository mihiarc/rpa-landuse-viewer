import sqlite3
from typing import Dict, Any
from pathlib import Path
from .config import Config

class DatabaseConnection:
    _connection = None
    
    @classmethod
    def get_connection(cls):
        if cls._connection is None:
            try:
                db_path = Config.get_db_config()['database_path']
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)
                cls._connection = sqlite3.connect(db_path)
                # Enable foreign keys
                cls._connection.execute("PRAGMA foreign_keys = ON")
                # Configure connection to return rows as dictionaries
                cls._connection.row_factory = sqlite3.Row
            except Exception as err:
                raise Exception(f"Failed to create SQLite connection: {err}")
        return cls._connection
    
    @staticmethod
    def close_connection():
        """Close the SQLite connection if it exists"""
        if DatabaseConnection._connection:
            DatabaseConnection._connection.close()
            DatabaseConnection._connection = None
            
    @classmethod
    def reset_connection(cls):
        """Reset the connection - useful for testing"""
        cls.close_connection() 