"""
Database connection module for RPA land use viewer.
"""

import sqlite3
import logging
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'database_path': 'data/database/rpa_landuse.db'
}

class DatabaseConnection:
    """
    A class to manage SQLite database connections.
    
    This class uses thread-local storage to ensure each thread gets its own
    connection to the SQLite database, preventing threading issues that can
    occur when sharing a single connection across threads.
    
    Usage:
        conn = DatabaseConnection.get_connection()
        # use the connection
        DatabaseConnection.close_connection(conn)
    """
    
    # Thread-local storage to hold connections specific to each thread
    _local = threading.local()
    
    @classmethod
    def get_connection(cls):
        """
        Get a connection to the database.
        
        This method creates a thread-specific connection or returns an 
        existing one for the current thread.
        
        Returns:
            sqlite3.Connection: A connection to the database
        """
        # Get the database path from config
        db_path = DB_CONFIG['database_path']
        
        # Create parent directory if it doesn't exist
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Create a new connection for this thread if one doesn't already exist
        # We don't store the connection in thread-local storage to ensure
        # each call gets a fresh connection, avoiding potential thread issues
        logger.info(f"Creating new connection to SQLite database at {db_path}")
        try:
            connection = sqlite3.connect(db_path)
            # Enable foreign keys
            connection.execute("PRAGMA foreign_keys = ON")
            return connection
        except sqlite3.Error as err:
            logger.error(f"Error connecting to database: {err}")
            raise
    
    @staticmethod
    def close_connection(conn):
        """
        Close the database connection.
        
        Args:
            conn (sqlite3.Connection): The connection to close.
        """
        if conn is not None:
            try:
                conn.close()
                logger.debug("Closed database connection")
            except sqlite3.Error as err:
                logger.error(f"Error closing database connection: {err}") 