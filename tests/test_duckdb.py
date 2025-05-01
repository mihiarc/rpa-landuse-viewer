#!/usr/bin/env python
"""
DuckDB Database Test Script
--------------------------
Tests the DuckDB database for the RPA Land Use Viewer application
independent of the Streamlit application.
"""

import os
import pytest
import duckdb
import pandas as pd
import logging
from pathlib import Path
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("duckdb_test")

# Load environment variables from .env file if present
load_dotenv()

# Database configuration
DB_PATH = os.getenv("DB_PATH", "data/database/rpa_landuse_duck.db")

class DuckDBTester:
    """Test class for DuckDB database operations."""
    
    def __init__(self, db_path=DB_PATH):
        """Initialize the tester with database path."""
        self.db_path = db_path
        # Create parent directory if it doesn't exist
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = None
    
    def connect(self):
        """Connect to the DuckDB database."""
        try:
            logger.info(f"Connecting to DuckDB database at {self.db_path}")
            self.conn = duckdb.connect(self.db_path)
            # Enable parallelism with at least 1 thread (or auto-detect optimal number)
            self.conn.execute("PRAGMA threads=4")
            return True
        except Exception as e:
            logger.error(f"Error connecting to DuckDB database: {e}")
            return False
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Database connection closed")
    
    def test_connection(self):
        """Test if connection can be established."""
        result = self.connect()
        self.close()
        return result
    
    def test_query(self, query):
        """Test a SQL query and return result."""
        if not self.conn:
            self.connect()
        
        try:
            logger.info(f"Executing query: {query}")
            result = self.conn.execute(query).fetchall()
            logger.info(f"Query executed successfully. Returned {len(result)} rows.")
            return result
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return None
    
    def get_tables(self):
        """Get list of tables in the database."""
        return self.test_query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
    
    def get_table_schema(self, table_name):
        """Get schema information for a table."""
        return self.test_query(f"DESCRIBE {table_name}")
    
    def test_table_row_count(self, table_name):
        """Test row count for a specific table."""
        result = self.test_query(f"SELECT COUNT(*) FROM {table_name}")
        if result:
            count = result[0][0]
            logger.info(f"Table {table_name} has {count} rows")
            return count
        return 0
    
    def test_sample_data(self, table_name, limit=5):
        """Get sample data from a table."""
        return self.test_query(f"SELECT * FROM {table_name} LIMIT {limit}")
    
    def test_scenarios_table(self):
        """Test the scenarios table structure and data."""
        schema = self.get_table_schema("scenarios")
        count = self.test_table_row_count("scenarios")
        sample = self.test_sample_data("scenarios")
        return {
            "schema": schema,
            "count": count,
            "sample": sample
        }
    
    def test_time_steps_table(self):
        """Test the time_steps table structure and data."""
        schema = self.get_table_schema("time_steps")
        count = self.test_table_row_count("time_steps")
        sample = self.test_sample_data("time_steps")
        return {
            "schema": schema,
            "count": count,
            "sample": sample
        }
    
    def test_counties_table(self):
        """Test the counties table structure and data."""
        schema = self.get_table_schema("counties")
        count = self.test_table_row_count("counties")
        sample = self.test_sample_data("counties")
        return {
            "schema": schema,
            "count": count,
            "sample": sample
        }
    
    def test_land_use_transitions_table(self):
        """Test the land_use_transitions table structure and data."""
        schema = self.get_table_schema("land_use_transitions")
        count = self.test_table_row_count("land_use_transitions")
        sample = self.test_sample_data("land_use_transitions")
        return {
            "schema": schema,
            "count": count,
            "sample": sample
        }
    
    def test_complex_query(self):
        """Test a more complex analytical query."""
        query = """
        SELECT
            s.scenario_name,
            t.start_year,
            t.end_year,
            lut.from_land_use,
            lut.to_land_use,
            SUM(lut.acres) as total_acres
        FROM
            land_use_transitions lut
        JOIN
            scenarios s ON lut.scenario_id = s.scenario_id
        JOIN
            time_steps t ON lut.time_step_id = t.time_step_id
        WHERE
            t.start_year >= 2020
        GROUP BY
            s.scenario_name, t.start_year, t.end_year, lut.from_land_use, lut.to_land_use
        ORDER BY
            total_acres DESC
        LIMIT 10
        """
        return self.test_query(query)
    
    def create_test_dataframe(self):
        """Create a test dataframe with land use data for testing."""
        data = {
            'scenario_id': [1, 1, 1, 2, 2],
            'time_step_id': [1, 1, 2, 2, 2],
            'fips_code': ['01001', '01001', '01002', '01002', '01003'],
            'from_land_use': ['Cropland', 'Forest', 'Pasture', 'Urban', 'Rangeland'],
            'to_land_use': ['Urban', 'Cropland', 'Forest', 'Forest', 'Urban'],
            'acres': [100.5, 250.3, 75.8, 120.2, 300.0]
        }
        return pd.DataFrame(data)
    
    def test_dataframe_query(self):
        """Test querying a pandas dataframe using DuckDB."""
        try:
            df = self.create_test_dataframe()
            logger.info("Created test dataframe")
            
            if not self.conn:
                self.connect()
            
            # First register the dataframe properly
            self.conn.register("df_view", df)
            
            # Query using the registered view
            query = """
            SELECT
                from_land_use,
                to_land_use,
                SUM(acres) as total_acres
            FROM
                df_view
            GROUP BY
                from_land_use, to_land_use
            ORDER BY
                total_acres DESC
            """
            
            result = self.conn.execute(query).fetchall()
            logger.info(f"Dataframe query executed successfully. Returned {len(result)} rows.")
            return result
        except Exception as e:
            logger.error(f"Dataframe query error: {e}")
            return None
    
    def run_all_tests(self):
        """Run all database tests."""
        results = {}
        
        # Test connection
        results["connection"] = self.test_connection()
        
        if not results["connection"]:
            logger.error("Failed to connect to the database. Aborting tests.")
            return results
        
        self.connect()
        
        # Basic database information
        results["tables"] = self.get_tables()
        
        # Test main tables
        try:
            results["scenarios_test"] = self.test_scenarios_table()
            results["time_steps_test"] = self.test_time_steps_table()
            results["counties_test"] = self.test_counties_table()
            results["land_use_transitions_test"] = self.test_land_use_transitions_table()
        except Exception as e:
            logger.error(f"Error testing tables: {e}")
        
        # Test complex query
        try:
            results["complex_query"] = self.test_complex_query()
        except Exception as e:
            logger.error(f"Error with complex query: {e}")
        
        # Test pandas dataframe with DuckDB
        results["dataframe_query"] = self.test_dataframe_query()
        
        self.close()
        return results


def print_test_results(results):
    """Print the test results in a readable format."""
    print("\n" + "="*80)
    print("DUCKDB DATABASE TEST RESULTS")
    print("="*80)
    
    # Connection test
    print(f"\nConnection test: {'PASSED' if results.get('connection') else 'FAILED'}")
    
    # Tables in the database
    if "tables" in results and results["tables"]:
        print(f"\nTables in the database ({len(results['tables'])}):")
        for table in results["tables"]:
            print(f"  - {table[0]}")
    
    # Table tests
    for table_name in ["scenarios", "time_steps", "counties", "land_use_transitions"]:
        test_key = f"{table_name}_test"
        if test_key in results and results[test_key]:
            print(f"\n{table_name.upper()} Table:")
            print(f"  - Row count: {results[test_key]['count']}")
            if results[test_key]['sample']:
                print(f"  - Sample data: {len(results[test_key]['sample'])} rows")
    
    # Complex query test
    if "complex_query" in results and results["complex_query"]:
        print("\nComplex query test: PASSED")
        print(f"  - Returned {len(results['complex_query'])} rows")
    else:
        print("\nComplex query test: FAILED")
    
    # Dataframe query test
    if "dataframe_query" in results and results["dataframe_query"]:
        print("\nDataframe query test: PASSED")
        print(f"  - Returned {len(results['dataframe_query'])} rows")
        print("  - Results:")
        for row in results["dataframe_query"]:
            print(f"    {row}")
    else:
        print("\nDataframe query test: FAILED")


def main():
    """Main function to run the tests."""
    tester = DuckDBTester()
    results = tester.run_all_tests()
    print_test_results(results)


if __name__ == "__main__":
    main() 