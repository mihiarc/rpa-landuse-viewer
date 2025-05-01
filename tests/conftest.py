import pytest
import sys
import os
from pathlib import Path

# Add the src directory to the path so we can import modules
sys.path.append(str(Path(__file__).parent.parent))
from src.db.database import DatabaseConnection

# Define database path - allow overriding from environment
DB_PATH = os.getenv('DB_PATH', 'data/database/rpa_landuse_duck.db')

@pytest.fixture(scope="session")
def db_path():
    """Return the database path for tests."""
    return DB_PATH

@pytest.fixture(scope="session", autouse=True)
def setup_database():
    """Setup database connection for tests and teardown after tests."""
    # Setup phase - nothing special needed as DatabaseConnection handles this
    print("Setting up database connection for tests")
    
    # The fixture yields control to the tests
    yield
    
    # Teardown phase - no explicit cleanup needed since we close connections after each use
    print("Tearing down database connection")
    # DatabaseConnection handles its own cleanup

@pytest.fixture(scope="session")
def db_connection():
    """Return a database connection for tests."""
    conn = DatabaseConnection.get_connection()
    yield conn
    # Close the connection when the session ends
    DatabaseConnection.close_connection(conn)

@pytest.fixture(scope="function")
def db_cursor(db_connection):
    """Return a database cursor for tests."""
    cursor = db_connection.cursor()
    yield cursor
    # No need to close the cursor as it's associated with the connection
    # that will be closed by db_connection fixture

@pytest.fixture(scope="session")
def sample_scenario(db_connection):
    """Return a sample scenario for tests."""
    result = db_connection.execute("SELECT scenario_id, scenario_name FROM scenarios LIMIT 1").fetchone()
    return {"scenario_id": result[0], "scenario_name": result[1]}

@pytest.fixture(scope="session")
def sample_time_step(db_connection):
    """Return a sample time step for tests."""
    result = db_connection.execute("SELECT time_step_id, start_year, end_year FROM time_steps LIMIT 1").fetchone()
    return {"time_step_id": result[0], "start_year": result[1], "end_year": result[2]}

@pytest.fixture(scope="session")
def sample_county(db_connection):
    """Return a sample county for tests."""
    result = db_connection.execute("SELECT fips_code, county_name FROM counties LIMIT 1").fetchone()
    return {"fips_code": result[0], "county_name": result[1]}

@pytest.fixture(scope="session")
def sample_land_use_types(db_connection):
    """Return sample land use types for tests."""
    land_use_types = db_connection.execute("""
        SELECT DISTINCT from_land_use FROM land_use_transitions 
        UNION SELECT DISTINCT to_land_use FROM land_use_transitions
        LIMIT 5
    """).fetchdf()['from_land_use'].tolist()
    return land_use_types

@pytest.fixture(scope="function")
def test_data(sample_scenario, sample_time_step, sample_county, sample_land_use_types):
    """Combine sample data for tests."""
    return {
        'scenario_id': sample_scenario["scenario_id"],
        'scenario_name': sample_scenario["scenario_name"],
        'start_year': sample_time_step["start_year"],
        'end_year': sample_time_step["end_year"],
        'county_fips': sample_county["fips_code"],
        'land_use_types': sample_land_use_types
    } 