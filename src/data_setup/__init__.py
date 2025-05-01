"""
Data setup module for RPA Land Use Viewer.
Contains simplified tools for one-time data processing and database setup.
"""

from .db_loader import load_to_duckdb, initialize_database, create_views, get_db_connection

__version__ = "1.0.0" 