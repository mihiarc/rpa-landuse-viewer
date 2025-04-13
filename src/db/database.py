"""
Database connection module for RPA land use viewer.
"""

import sqlite3
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'database_path': 'data/database/rpa_landuse.db'
}

class DatabaseConnection:
    """
    A class to manage SQLite database connections.
    """
    _connection = None
    
    @classmethod
    def get_connection(cls):
        """
        Get a connection to the database.
        
        Returns:
            sqlite3.Connection: A connection to the database
        """
        if cls._connection is None:
            db_path = DB_CONFIG['database_path']
            
            # Create parent directory if it doesn't exist
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Connecting to SQLite database at {db_path}")
            try:
                cls._connection = sqlite3.connect(db_path)
                # Enable foreign keys
                cls._connection.execute("PRAGMA foreign_keys = ON")
            except sqlite3.Error as err:
                logger.error(f"Error connecting to database: {err}")
                raise
        
        return cls._connection
    
    @classmethod
    def close_connection(cls, conn=None):
        """
        Close the database connection.
        
        Args:
            conn (sqlite3.Connection, optional): The connection to close.
                If None, closes the class's stored connection.
        """
        if conn is not None:
            try:
                conn.close()
                logger.debug("Closed provided database connection")
            except sqlite3.Error as err:
                logger.error(f"Error closing database connection: {err}")
        elif cls._connection is not None:
            try:
                cls._connection.close()
                cls._connection = None
                logger.debug("Closed stored database connection")
            except sqlite3.Error as err:
                logger.error(f"Error closing database connection: {err}") 