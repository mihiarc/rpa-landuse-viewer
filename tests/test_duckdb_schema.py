#!/usr/bin/env python
"""
DuckDB Schema Validation Tests
-----------------------------
Tests to validate the database schema against expected definitions.
"""

import os
import pytest
import duckdb
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional

# Load environment variables
load_dotenv()

# Database configuration 
DB_PATH = os.getenv("DB_PATH", "data/database/rpa_landuse_duck.db")

# Expected schema definitions
EXPECTED_SCHEMAS = {
    "scenarios": {
        "columns": [
            {"name": "scenario_id", "type": "INTEGER", "nullable": False, "primary_key": True},
            {"name": "scenario_name", "type": "VARCHAR", "nullable": True},
            {"name": "gcm", "type": "VARCHAR", "nullable": True},
            {"name": "rcp", "type": "VARCHAR", "nullable": True},
            {"name": "ssp", "type": "VARCHAR", "nullable": True}
        ],
        "primary_key": "scenario_id"
    },
    "time_steps": {
        "columns": [
            {"name": "time_step_id", "type": "INTEGER", "nullable": False, "primary_key": True},
            {"name": "start_year", "type": "INTEGER", "nullable": True},
            {"name": "end_year", "type": "INTEGER", "nullable": True}
        ],
        "primary_key": "time_step_id"
    },
    "counties": {
        "columns": [
            {"name": "fips_code", "type": "VARCHAR", "nullable": False, "primary_key": True},
            {"name": "county_name", "type": "VARCHAR", "nullable": True}
        ],
        "primary_key": "fips_code"
    },
    "land_use_transitions": {
        "columns": [
            {"name": "transition_id", "type": "INTEGER", "nullable": False, "primary_key": True},
            {"name": "scenario_id", "type": "INTEGER", "nullable": True, "foreign_key": "scenarios.scenario_id"},
            {"name": "time_step_id", "type": "INTEGER", "nullable": True, "foreign_key": "time_steps.time_step_id"},
            {"name": "fips_code", "type": "VARCHAR", "nullable": True, "foreign_key": "counties.fips_code"},
            {"name": "from_land_use", "type": "VARCHAR", "nullable": True},
            {"name": "to_land_use", "type": "VARCHAR", "nullable": True},
            {"name": "acres", "type": "DOUBLE", "nullable": True}
        ],
        "primary_key": "transition_id",
        "foreign_keys": [
            {"column": "scenario_id", "references": "scenarios.scenario_id"},
            {"column": "time_step_id", "references": "time_steps.time_step_id"},
            {"column": "fips_code", "references": "counties.fips_code"}
        ]
    }
}


@pytest.fixture
def db_connection():
    """Create a connection to the database for testing."""
    try:
        conn = duckdb.connect(DB_PATH)
        yield conn
    finally:
        if conn:
            conn.close()


def get_table_columns(conn, table_name: str) -> List[Dict[str, Any]]:
    """Get column information for a table."""
    # Use DESCRIBE to get column information
    results = conn.execute(f"DESCRIBE {table_name}").fetchall()
    
    columns = []
    for row in results:
        column_info = {
            "name": row[0],
            "type": row[1],
            "nullable": not row[3] == "NOT NULL"
        }
        columns.append(column_info)
    
    return columns


def get_primary_key(conn, table_name: str) -> Optional[str]:
    """Get primary key of a table."""
    try:
        # This is a simplified version, actual DuckDB may require a different approach
        result = conn.execute(f"""
            SELECT column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
            WHERE tc.table_name = '{table_name}'
            AND tc.constraint_type = 'PRIMARY KEY'
        """).fetchone()
        
        return result[0] if result else None
    except:
        # DuckDB might not support information_schema queries in the same way
        # Return None if there's an error
        return None


def get_foreign_keys(conn, table_name: str) -> List[Dict[str, str]]:
    """Get foreign keys from a table."""
    try:
        # This is a simplified version, actual DuckDB may require a different approach
        results = conn.execute(f"""
            SELECT ccu.column_name, kcu.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu 
                ON tc.constraint_name = ccu.constraint_name
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = '{table_name}'
            AND tc.constraint_type = 'FOREIGN KEY'
        """).fetchall()
        
        foreign_keys = []
        for row in results:
            foreign_keys.append({
                "column": row[0],
                "references": f"{row[1]}.{row[2]}"
            })
        
        return foreign_keys
    except:
        # DuckDB might not support information_schema queries in the same way
        # Return empty list if there's an error
        return []


def test_tables_exist(db_connection):
    """Test that all expected tables exist in the database."""
    # Get list of tables in database
    tables = db_connection.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    
    table_names = [table[0] for table in tables]
    
    # Check if all expected tables exist
    for table_name in EXPECTED_SCHEMAS.keys():
        assert table_name in table_names, f"Table {table_name} does not exist in the database"


def test_column_schema(db_connection):
    """Test that all tables have the expected columns with correct types."""
    for table_name, expected_schema in EXPECTED_SCHEMAS.items():
        # Get actual columns
        actual_columns = get_table_columns(db_connection, table_name)
        
        # Check column count
        assert len(actual_columns) == len(expected_schema["columns"]), \
            f"Column count mismatch for table {table_name}. " \
            f"Expected {len(expected_schema['columns'])}, got {len(actual_columns)}"
        
        # Create a dictionary of actual columns for easier lookup
        actual_column_dict = {col["name"]: col for col in actual_columns}
        
        # Check each expected column
        for expected_column in expected_schema["columns"]:
            column_name = expected_column["name"]
            
            # Check if column exists
            assert column_name in actual_column_dict, \
                f"Column {column_name} does not exist in table {table_name}"
            
            actual_column = actual_column_dict[column_name]
            
            # Check type
            # Note: DuckDB might return slightly different type names, so use a startswith check
            assert actual_column["type"].upper().startswith(expected_column["type"].upper()), \
                f"Column {column_name} in table {table_name} has wrong type. " \
                f"Expected {expected_column['type']}, got {actual_column['type']}"
            
            # Check nullability if specified
            if "nullable" in expected_column:
                assert actual_column["nullable"] == expected_column["nullable"], \
                    f"Column {column_name} in table {table_name} has wrong nullability. " \
                    f"Expected nullable={expected_column['nullable']}, " \
                    f"got nullable={actual_column['nullable']}"


def test_sample_data(db_connection):
    """Test that tables have data."""
    for table_name in EXPECTED_SCHEMAS.keys():
        count = db_connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        assert count > 0, f"Table {table_name} has no data"


def test_basic_relationships(db_connection):
    """Test that foreign key relationships function properly."""
    # Test scenarios/land_use_transitions relationship
    result = db_connection.execute("""
        SELECT COUNT(*) 
        FROM land_use_transitions lut
        JOIN scenarios s ON lut.scenario_id = s.scenario_id
    """).fetchone()[0]
    
    total_transitions = db_connection.execute(
        "SELECT COUNT(*) FROM land_use_transitions"
    ).fetchone()[0]
    
    assert result == total_transitions, \
        "Not all land_use_transitions records have a corresponding scenarios record"
    
    # Test time_steps/land_use_transitions relationship
    result = db_connection.execute("""
        SELECT COUNT(*) 
        FROM land_use_transitions lut
        JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
    """).fetchone()[0]
    
    assert result == total_transitions, \
        "Not all land_use_transitions records have a corresponding time_steps record"
    
    # Test counties/land_use_transitions relationship
    result = db_connection.execute("""
        SELECT COUNT(*) 
        FROM land_use_transitions lut
        JOIN counties c ON lut.fips_code = c.fips_code
    """).fetchone()[0]
    
    assert result == total_transitions, \
        "Not all land_use_transitions records have a corresponding counties record"


def test_unique_constraints(db_connection):
    """Test that primary keys enforce uniqueness."""
    for table_name, schema in EXPECTED_SCHEMAS.items():
        if "primary_key" in schema:
            primary_key = schema["primary_key"]
            
            # Count distinct values of primary key
            distinct_count = db_connection.execute(
                f"SELECT COUNT(DISTINCT {primary_key}) FROM {table_name}"
            ).fetchone()[0]
            
            # Count total rows
            total_count = db_connection.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]
            
            # If these match, then the primary key is unique
            assert distinct_count == total_count, \
                f"Primary key {primary_key} in table {table_name} is not enforcing uniqueness"


if __name__ == "__main__":
    pytest.main(["-xvs", __file__]) 