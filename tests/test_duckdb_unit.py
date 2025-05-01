#!/usr/bin/env python
"""
DuckDB Database Unit Tests
-------------------------
Unit tests for the DuckDB database for the RPA Land Use Viewer application.
"""

import os
import pytest
import duckdb
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Test database will be created in-memory for isolation
TEST_DB_PATH = ":memory:"


@pytest.fixture
def db_connection():
    """Fixture to create and return a DuckDB connection for testing."""
    conn = duckdb.connect(database=TEST_DB_PATH)
    # Enable parallelism (with at least 1 thread)
    conn.execute("PRAGMA threads=1")
    yield conn
    conn.close()


@pytest.fixture
def setup_test_schema(db_connection):
    """Fixture to create a test schema with tables that match our RPA land use database."""
    # Create scenarios table
    db_connection.execute("""
        CREATE TABLE scenarios (
            scenario_id INTEGER PRIMARY KEY,
            scenario_name VARCHAR,
            gcm VARCHAR,
            rcp VARCHAR,
            ssp VARCHAR
        )
    """)
    
    # Create time_steps table
    db_connection.execute("""
        CREATE TABLE time_steps (
            time_step_id INTEGER PRIMARY KEY,
            start_year INTEGER,
            end_year INTEGER
        )
    """)
    
    # Create counties table
    db_connection.execute("""
        CREATE TABLE counties (
            fips_code VARCHAR PRIMARY KEY,
            county_name VARCHAR
        )
    """)
    
    # Create land_use_transitions table
    db_connection.execute("""
        CREATE TABLE land_use_transitions (
            transition_id INTEGER PRIMARY KEY,
            scenario_id INTEGER,
            time_step_id INTEGER,
            fips_code VARCHAR,
            from_land_use VARCHAR,
            to_land_use VARCHAR,
            acres DOUBLE,
            FOREIGN KEY (scenario_id) REFERENCES scenarios(scenario_id),
            FOREIGN KEY (time_step_id) REFERENCES time_steps(time_step_id),
            FOREIGN KEY (fips_code) REFERENCES counties(fips_code)
        )
    """)
    
    return db_connection


@pytest.fixture
def populate_test_data(setup_test_schema):
    """Fixture to populate test tables with sample data."""
    conn = setup_test_schema
    
    # Insert sample scenarios
    scenarios_data = [
        (1, 'CNRM_CM5_rcp45_ssp1', 'CNRM_CM5', 'rcp45', 'ssp1'),
        (2, 'HadGEM2_ES365_rcp85_ssp2', 'HadGEM2_ES365', 'rcp85', 'ssp2'),
        (3, 'IPSL_CM5A_MR_rcp85_ssp3', 'IPSL_CM5A_MR', 'rcp85', 'ssp3')
    ]
    
    for scenario in scenarios_data:
        conn.execute(
            "INSERT INTO scenarios (scenario_id, scenario_name, gcm, rcp, ssp) VALUES (?, ?, ?, ?, ?)",
            scenario
        )
    
    # Insert sample time steps
    time_steps_data = [
        (1, 2020, 2030),
        (2, 2030, 2040),
        (3, 2040, 2050)
    ]
    
    for time_step in time_steps_data:
        conn.execute(
            "INSERT INTO time_steps (time_step_id, start_year, end_year) VALUES (?, ?, ?)",
            time_step
        )
    
    # Insert sample counties
    counties_data = [
        ('01001', 'Autauga County'),
        ('01003', 'Baldwin County'),
        ('01005', 'Barbour County')
    ]
    
    for county in counties_data:
        conn.execute(
            "INSERT INTO counties (fips_code, county_name) VALUES (?, ?)",
            county
        )
    
    # Insert sample land use transitions
    transitions_data = [
        (1, 1, 1, '01001', 'Cropland', 'Urban', 150.5),
        (2, 1, 1, '01001', 'Forest', 'Cropland', 220.3),
        (3, 1, 2, '01003', 'Pasture', 'Forest', 85.7),
        (4, 2, 2, '01003', 'Urban', 'Forest', 25.2),
        (5, 3, 3, '01005', 'Rangeland', 'Urban', 340.1)
    ]
    
    for transition in transitions_data:
        conn.execute(
            """
            INSERT INTO land_use_transitions 
            (transition_id, scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            transition
        )
    
    return conn


@pytest.fixture
def sample_dataframe():
    """Fixture that creates a sample DataFrame for testing."""
    data = {
        'scenario_id': [1, 1, 1, 2, 2],
        'time_step_id': [1, 1, 2, 2, 2],
        'fips_code': ['01001', '01001', '01003', '01003', '01005'],
        'from_land_use': ['Cropland', 'Forest', 'Pasture', 'Urban', 'Rangeland'],
        'to_land_use': ['Urban', 'Cropland', 'Forest', 'Forest', 'Urban'],
        'acres': [100.5, 250.3, 75.8, 120.2, 300.0]
    }
    return pd.DataFrame(data)


def test_connection():
    """Test that we can connect to an in-memory DuckDB database."""
    conn = duckdb.connect(":memory:")
    assert conn is not None
    conn.close()


def test_table_creation(setup_test_schema):
    """Test that tables were created properly."""
    conn = setup_test_schema
    
    # Check if tables exist
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    
    table_names = [table[0] for table in tables]
    
    assert 'scenarios' in table_names
    assert 'time_steps' in table_names
    assert 'counties' in table_names
    assert 'land_use_transitions' in table_names


def test_data_insertion(populate_test_data):
    """Test that data was inserted correctly."""
    conn = populate_test_data
    
    # Check count of rows in each table
    scenarios_count = conn.execute("SELECT COUNT(*) FROM scenarios").fetchone()[0]
    time_steps_count = conn.execute("SELECT COUNT(*) FROM time_steps").fetchone()[0]
    counties_count = conn.execute("SELECT COUNT(*) FROM counties").fetchone()[0]
    transitions_count = conn.execute("SELECT COUNT(*) FROM land_use_transitions").fetchone()[0]
    
    assert scenarios_count == 3
    assert time_steps_count == 3
    assert counties_count == 3
    assert transitions_count == 5


def test_basic_query(populate_test_data):
    """Test basic queries on the test database."""
    conn = populate_test_data
    
    # Test a simple query
    result = conn.execute(
        "SELECT scenario_name FROM scenarios WHERE gcm = 'CNRM_CM5'"
    ).fetchall()
    
    assert len(result) == 1
    assert result[0][0] == 'CNRM_CM5_rcp45_ssp1'


def test_join_query(populate_test_data):
    """Test a join query to validate relationships."""
    conn = populate_test_data
    
    # Test a join query
    result = conn.execute("""
        SELECT 
            s.scenario_name,
            t.start_year,
            t.end_year,
            c.county_name,
            lut.from_land_use,
            lut.to_land_use,
            lut.acres
        FROM 
            land_use_transitions lut
        JOIN 
            scenarios s ON lut.scenario_id = s.scenario_id
        JOIN 
            time_steps t ON lut.time_step_id = t.time_step_id
        JOIN 
            counties c ON lut.fips_code = c.fips_code
        WHERE 
            lut.acres > 200
        ORDER BY 
            lut.acres DESC
    """).fetchall()
    
    # Should be 3 records with acres > 200
    assert len(result) == 3
    
    # Check that the first result is the largest transition
    assert result[0][6] > 300


def test_aggregation_query(populate_test_data):
    """Test aggregation queries."""
    conn = populate_test_data
    
    # Test an aggregation query
    result = conn.execute("""
        SELECT 
            from_land_use,
            to_land_use,
            SUM(acres) as total_acres
        FROM 
            land_use_transitions
        GROUP BY 
            from_land_use, to_land_use
        ORDER BY 
            total_acres DESC
    """).fetchall()
    
    # Verify the results
    assert len(result) == 5  # Should have 5 distinct from/to combinations
    
    # The largest transition should be from Rangeland to Urban (340.1 acres)
    assert result[0][0] == 'Rangeland'
    assert result[0][1] == 'Urban'
    assert round(result[0][2], 1) == 340.1


def test_dataframe_query(db_connection, sample_dataframe):
    """Test querying a pandas dataframe using DuckDB."""
    conn = db_connection
    
    # Test query on the dataframe
    result = conn.execute("""
        SELECT 
            from_land_use, 
            to_land_use, 
            SUM(acres) as total_acres
        FROM 
            sample_dataframe
        GROUP BY 
            from_land_use, to_land_use
        ORDER BY 
            total_acres DESC
    """, {"sample_dataframe": sample_dataframe}).fetchall()
    
    # Verify results
    assert len(result) == 5
    
    # The largest transition should be from Rangeland to Urban (300.0 acres)
    assert result[0][0] == 'Rangeland'
    assert result[0][1] == 'Urban'
    assert result[0][2] == 300.0


def test_transaction_rollback(db_connection):
    """Test transaction management with rollback."""
    conn = db_connection
    
    # Create a simple test table
    conn.execute("CREATE TABLE test_transactions (id INTEGER, value VARCHAR)")
    
    # Start a transaction
    conn.execute("BEGIN TRANSACTION")
    
    # Insert some data
    conn.execute("INSERT INTO test_transactions VALUES (1, 'Original Value')")
    
    # Check that data is visible within the transaction
    result = conn.execute("SELECT value FROM test_transactions WHERE id = 1").fetchone()
    assert result[0] == 'Original Value'
    
    # Rollback the transaction
    conn.execute("ROLLBACK")
    
    # Verify the data was rolled back
    result = conn.execute("SELECT COUNT(*) FROM test_transactions").fetchone()
    assert result[0] == 0


def test_transaction_commit(db_connection):
    """Test transaction management with commit."""
    conn = db_connection
    
    # Create a simple test table
    conn.execute("CREATE TABLE test_transactions (id INTEGER, value VARCHAR)")
    
    # Start a transaction
    conn.execute("BEGIN TRANSACTION")
    
    # Insert some data
    conn.execute("INSERT INTO test_transactions VALUES (1, 'Original Value')")
    
    # Commit the transaction
    conn.execute("COMMIT")
    
    # Verify the data was committed
    result = conn.execute("SELECT value FROM test_transactions WHERE id = 1").fetchone()
    assert result[0] == 'Original Value'


if __name__ == "__main__":
    pytest.main(["-xvs", __file__]) 