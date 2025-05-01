import os
import pytest
import duckdb
from pathlib import Path
import sys
import pandas as pd
import numpy as np

# Add the src directory to the path so we can import modules
sys.path.append(str(Path(__file__).parent.parent))
from src.db.database import DatabaseConnection

# Database configuration
DB_CONFIG = {
    'database_path': os.getenv('DB_PATH', 'data/database/rpa_landuse_duck.db')
}

# Define expected tables and their expected row counts (approximate)
EXPECTED_TABLES = [
    'scenarios',
    'time_steps',
    'counties',
    'land_use_transitions'
]

EXPECTED_MIN_ROW_COUNTS = {
    'scenarios': 15,          # Should have at least 15 scenarios
    'time_steps': 5,          # Should have at least 5 time steps
    'counties': 3000,         # Should have at least 3000 counties
    'land_use_transitions': 5000000  # Should have at least 5 million transitions
}


def test_db_file_exists():
    """Test that the DuckDB database file exists."""
    db_path = DB_CONFIG['database_path']
    assert os.path.exists(db_path), f"Database file doesn't exist at {db_path}"
    assert os.path.getsize(db_path) > 1000000, "Database file is too small, might not have data"


def test_db_connection():
    """Test that we can connect to the database."""
    conn = DatabaseConnection.get_connection()
    assert conn is not None, "Failed to connect to database"
    version = conn.execute("SELECT version()").fetchone()[0]
    assert version is not None, "Failed to get DuckDB version"
    print(f"DuckDB version: {version}")


def test_tables_exist():
    """Test that all expected tables exist in the database."""
    conn = DatabaseConnection.get_connection()
    
    # Get list of tables
    result = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
    """).fetchall()
    tables = [row[0] for row in result]
    
    # Check all expected tables exist
    for table in EXPECTED_TABLES:
        assert table in tables, f"Table '{table}' not found in database"


def test_table_row_counts():
    """Test that tables have the expected number of rows."""
    conn = DatabaseConnection.get_connection()
    
    for table, min_count in EXPECTED_MIN_ROW_COUNTS.items():
        result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        count = result[0]
        assert count >= min_count, f"Table '{table}' has {count} rows, expected at least {min_count}"
        print(f"Table '{table}' has {count} rows")


def test_scenario_data():
    """Test that scenario data is valid."""
    conn = DatabaseConnection.get_connection()
    
    # Check scenarios have correct structure
    scenarios = conn.execute("SELECT scenario_id, scenario_name, gcm, rcp, ssp FROM scenarios LIMIT 5").fetchall()
    assert len(scenarios) > 0, "No scenarios found"
    
    # Check a scenario has valid data
    for scenario in scenarios:
        scenario_id, scenario_name, gcm, rcp, ssp = scenario
        assert scenario_id is not None, "scenario_id is None"
        assert scenario_name is not None, "scenario_name is None"
        assert gcm is not None, "gcm is None"
        assert rcp is not None, "rcp is None"
        assert ssp is not None, "ssp is None"
        
        # Check format
        assert "_" in scenario_name, f"Invalid scenario name format: {scenario_name}"
        parts = scenario_name.split("_")
        assert len(parts) >= 3, f"Invalid scenario name parts: {parts}"


def test_time_steps():
    """Test that time steps are valid."""
    conn = DatabaseConnection.get_connection()
    
    time_steps = conn.execute("SELECT time_step_id, start_year, end_year FROM time_steps").fetchall()
    assert len(time_steps) > 0, "No time steps found"
    
    for step in time_steps:
        time_step_id, start_year, end_year = step
        assert time_step_id is not None, "time_step_id is None"
        assert start_year is not None, "start_year is None"
        assert end_year is not None, "end_year is None"
        assert int(end_year) > int(start_year), f"Invalid time step: {start_year}-{end_year}"


def test_counties():
    """Test that counties are valid."""
    conn = DatabaseConnection.get_connection()
    
    counties = conn.execute("SELECT fips_code, county_name FROM counties LIMIT 10").fetchall()
    assert len(counties) > 0, "No counties found"
    
    for county in counties:
        fips_code, county_name = county
        assert fips_code is not None, "fips_code is None"
        assert county_name is not None, "county_name is None"
        assert len(fips_code) == 5, f"Invalid FIPS code length: {fips_code}"


def test_land_use_transitions():
    """Test that land use transitions are valid."""
    conn = DatabaseConnection.get_connection()
    
    transitions = conn.execute("""
        SELECT 
            t.transition_id, 
            t.scenario_id, 
            t.time_step_id, 
            t.fips_code, 
            t.from_land_use, 
            t.to_land_use, 
            t.acres,
            s.scenario_name,
            ts.start_year,
            ts.end_year
        FROM land_use_transitions t
        JOIN scenarios s ON t.scenario_id = s.scenario_id
        JOIN time_steps ts ON t.time_step_id = ts.time_step_id
        LIMIT 5
    """).fetchall()
    assert len(transitions) > 0, "No land use transitions found"
    
    # Check the first few transitions
    for transition in transitions:
        assert transition[0] is not None, "transition_id is None"
        assert transition[1] is not None, "scenario_id is None"
        assert transition[2] is not None, "time_step_id is None"
        assert transition[3] is not None, "fips_code is None"
        assert transition[4] is not None, "from_land_use is None"
        assert transition[5] is not None, "to_land_use is None"
        assert transition[6] is not None, "acres is None"
        assert transition[7] is not None, "scenario_name is None"
        assert transition[8] is not None, "start_year is None"
        assert transition[9] is not None, "end_year is None"


def test_simple_query():
    """Test that we can run a simple query."""
    conn = DatabaseConnection.get_connection()
    
    query = """
        SELECT 
            s.scenario_name,
            ts.start_year, 
            ts.end_year,
            COUNT(*) as transition_count
        FROM land_use_transitions t
        JOIN scenarios s ON t.scenario_id = s.scenario_id
        JOIN time_steps ts ON t.time_step_id = ts.time_step_id
        GROUP BY s.scenario_name, ts.start_year, ts.end_year
        LIMIT 5
    """
    
    results = conn.execute(query).fetchall()
    assert len(results) > 0, "No results from simple query"
    
    for result in results:
        scenario_name, start_year, end_year, count = result
        assert scenario_name is not None, "scenario_name is None"
        assert start_year is not None, "start_year is None"
        assert end_year is not None, "end_year is None"
        assert count > 0, f"Zero transitions for {scenario_name} in {start_year}-{end_year}"
        print(f"Scenario {scenario_name} ({start_year}-{end_year}): {count} transitions")


if __name__ == "__main__":
    # Rename the test file to match the new functionality
    print("Running DuckDB database tests...")
    pytest.main(["-v", __file__]) 