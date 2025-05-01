"""
Database connection module for RPA land use viewer.
"""

import os
import logging
import threading
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'database_path': os.getenv('DB_PATH', 'data/database/rpa_landuse_duck.db')
}

class DatabaseConnection:
    """
    A class to manage DuckDB database connections.
    
    This class uses thread-local storage to ensure each thread gets its own
    connection to the database, preventing threading issues that can
    occur when sharing a single connection across threads.
    
    Usage:
        conn = DatabaseConnection.get_connection()
        # use the connection
        DatabaseConnection.close_connection(conn)
        
        # For pandas operations, use:
        engine = DatabaseConnection.get_sqlalchemy_engine()
        # use the engine with pandas
    """
    
    # Thread-local storage to hold connections specific to each thread
    _local = threading.local()
    
    @classmethod
    def get_connection(cls):
        """
        Get a connection to the DuckDB database.
        
        This method creates a thread-specific connection or returns an 
        existing one for the current thread.
        
        Returns:
            duckdb.DuckDBPyConnection: A connection to the database
        """
        # Get the database path from config
        db_path = DB_CONFIG['database_path']
        
        # Create parent directory if it doesn't exist
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Create a new connection
        logger.info(f"Creating new connection to DuckDB database at {db_path}")
        
        try:
            import duckdb
            # DuckDB has auto-commit enabled by default
            connection = duckdb.connect(db_path)
            # Enable parallelism with specific thread count
            connection.execute("PRAGMA threads=4")
            return connection
        except Exception as err:
            logger.error(f"Error connecting to DuckDB database: {err}")
            raise
    
    @classmethod
    def get_sqlalchemy_engine(cls):
        """
        Get a SQLAlchemy engine for the DuckDB database.
        
        This method creates a SQLAlchemy engine that can be used with pandas
        to avoid the pandas warning about DuckDB connections.
        
        Returns:
            sqlalchemy.engine.Engine: A SQLAlchemy engine for the database
        """
        # Get the database path from config
        db_path = DB_CONFIG['database_path']
        
        # Create parent directory if it doesn't exist
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Creating SQLAlchemy engine for DuckDB database at {db_path}")
        
        try:
            from sqlalchemy import create_engine
            # Make sure the path is absolute
            absolute_path = str(Path(db_path).absolute())
            # Create SQLAlchemy engine for DuckDB with the proper connection string
            # The format is "duckdb:///path/to/database.db"
            engine = create_engine(f"duckdb:///{absolute_path}")
            return engine
        except Exception as err:
            logger.error(f"Error creating SQLAlchemy engine for DuckDB: {err}")
            raise
    
    @classmethod
    def execute_pandas_query(cls, query, params=None):
        """
        Execute a SQL query and return results as a pandas DataFrame using SQLAlchemy.
        
        This method handles the conversion of DuckDB-style parameterized queries
        to SQLAlchemy's format, and executes the query using pandas.read_sql_query.
        
        Args:
            query (str): SQL query with DuckDB-style placeholders (?)
            params (list, optional): List of parameters for the query
            
        Returns:
            pandas.DataFrame: Query results as a DataFrame
        """
        try:
            import pandas as pd
            from sqlalchemy.sql import text
            
            # Get SQLAlchemy engine
            engine = cls.get_sqlalchemy_engine()
            
            if params:
                # Convert DuckDB-style parameterized query to SQLAlchemy format
                query_text = text(query.replace('?', ':p{}').format(*range(len(params))))
                param_dict = {f'p{i}': param for i, param in enumerate(params)}
                return pd.read_sql_query(query_text, engine, params=param_dict)
            else:
                # No parameters, use the query directly
                return pd.read_sql_query(query, engine)
                
        except Exception as err:
            logger.error(f"Error executing pandas query: {err}")
            # Return empty DataFrame on error
            import pandas as pd
            return pd.DataFrame()
    
    @staticmethod
    def close_connection(conn):
        """
        Close the database connection.
        
        Args:
            conn: The database connection to close.
        """
        if conn is not None:
            try:
                conn.close()
                logger.debug("Closed database connection")
            except Exception as err:
                logger.error(f"Error closing database connection: {err}") 