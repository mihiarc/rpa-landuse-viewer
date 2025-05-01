#!/usr/bin/env python
"""
Diagnostic script to test state grouping functionality in the RPA Land Use database.
This script performs various queries directly against the database to verify the 
correctness of state data, relationships, and aggregation.
"""

import os
import sys
import pytest
import logging
import duckdb
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
DB_PATH = os.getenv('DB_PATH', str(Path(__file__).parent.parent / 'data' / 'database' / 'rpa_landuse_duck.db'))

def setup_module(module):
    """Set up the database for testing."""
    logger.info(f"Using database at {DB_PATH}")
    
    # Make sure the parent directory exists
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

@pytest.fixture
def db_connection():
    """Fixture for a database connection."""
    conn = duckdb.connect(DB_PATH)
    yield conn
    conn.close()

def test_states_table_exists(db_connection):
    """Test that the states table exists."""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'main' AND table_name = 'states'
    """
    result = db_connection.execute(query).fetchall()
    assert len(result) == 1, "States table not found"
    assert result[0][0] == 'states', "States table has wrong name"

def test_states_populated(db_connection):
    """Test that the states table is populated."""
    query = "SELECT COUNT(*) FROM states"
    result = db_connection.execute(query).fetchone()
    assert result[0] > 0, "States table is empty"
    
    # Check for some specific states
    query = "SELECT state_name FROM states WHERE state_fips IN ('06', '36', '48')"
    result = db_connection.execute(query).fetchall()
    state_names = [row[0] for row in result]
    assert "California" in state_names, "California not found"
    assert "New York" in state_names, "New York not found"
    assert "Texas" in state_names, "Texas not found"

def test_county_state_map_view(db_connection):
    """Test that the county_state_map view exists and works."""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'main' AND table_name = 'county_state_map'
    """
    result = db_connection.execute(query).fetchall()
    assert len(result) == 1, "county_state_map view not found"
    
    # Test the view returns records
    query = "SELECT COUNT(*) FROM county_state_map"
    result = db_connection.execute(query).fetchone()
    assert result[0] > 0, "county_state_map view is empty"
    
    # Test that counties are correctly mapped to states
    query = """
    SELECT state_name, state_abbr 
    FROM county_state_map 
    WHERE county_fips = '36001' -- Albany County, NY
    """
    result = db_connection.execute(query).fetchone()
    assert result is not None, "Albany County not found"
    assert result[0] == "New York", f"Wrong state name: {result[0]}"
    assert result[1] == "NY", f"Wrong state abbreviation: {result[1]}"

def test_state_land_use_transitions_view(db_connection):
    """Test that the state_land_use_transitions view exists and works."""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'main' AND table_name = 'state_land_use_transitions'
    """
    result = db_connection.execute(query).fetchall()
    assert len(result) == 1, "state_land_use_transitions view not found"
    
    # Test the view returns records (if data is loaded)
    query = "SELECT COUNT(*) FROM state_land_use_transitions"
    result = db_connection.execute(query).fetchone()
    logger.info(f"Found {result[0]} records in state_land_use_transitions")
    
    # If we have records, check some basic structure
    if result[0] > 0:
        query = """
        SELECT scenario_name, state_name, from_land_use, to_land_use, acres 
        FROM state_land_use_transitions 
        LIMIT 1
        """
        result = db_connection.execute(query).fetchone()
        assert result is not None, "No records returned from query"
        assert len(result) == 5, f"Wrong number of columns: {len(result)}"
        assert result[3] in ["Crop", "Forest", "Pasture", "Range", "Urban"], f"Unexpected to_land_use: {result[3]}"
        assert isinstance(result[4], (int, float)), f"Acres is not a number: {result[4]}"

def test_region_hierarchy_view(db_connection):
    """Test that the region_hierarchy view exists and works."""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'main' AND table_name = 'region_hierarchy'
    """
    result = db_connection.execute(query).fetchall()
    assert len(result) == 1, "region_hierarchy view not found"
    
    # Test the view returns records 
    query = "SELECT COUNT(*) FROM region_hierarchy"
    result = db_connection.execute(query).fetchone()
    assert result[0] > 0, "region_hierarchy view is empty"
    
    # Check distinct levels
    query = "SELECT DISTINCT level FROM region_hierarchy ORDER BY level"
    result = db_connection.execute(query).fetchall()
    levels = [row[0] for row in result]
    assert levels == [0, 1], f"Unexpected levels: {levels}"
    
    # Check region types
    query = "SELECT DISTINCT region_type FROM region_hierarchy ORDER BY region_type"
    result = db_connection.execute(query).fetchall()
    types = [row[0] for row in result]
    assert "COUNTY" in types, "COUNTY type not found"
    assert "STATE" in types, "STATE type not found"

def main():
    """Run the tests directly."""
    test_conn = duckdb.connect(DB_PATH)
    
    try:
        logger.info("Testing states table...")
        test_states_table_exists(test_conn)
        test_states_populated(test_conn)
        
        logger.info("Testing county_state_map view...")
        test_county_state_map_view(test_conn)
        
        logger.info("Testing state_land_use_transitions view...")
        test_state_land_use_transitions_view(test_conn)
        
        logger.info("Testing region_hierarchy view...")
        test_region_hierarchy_view(test_conn)
        
        logger.info("All tests passed!")
    except Exception as e:
        logger.error(f"Test failed: {e}")
    finally:
        test_conn.close()

if __name__ == "__main__":
    main() 