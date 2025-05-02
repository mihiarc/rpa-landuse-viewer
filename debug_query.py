"""
Debug script to check what's happening with the data query in Home.py
"""

import os
from dotenv import load_dotenv
import duckdb
import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).parent))

# Load environment variables
load_dotenv()

# Get the database path from environment
db_path = os.getenv('DB_PATH', 'data/database/rpa_landuse.db')
print(f"Using database: {db_path}")

# Connect to the database
conn = duckdb.connect(db_path)

# Check if land_use_summary table exists and has data
try:
    count = conn.execute("SELECT COUNT(*) FROM land_use_summary").fetchone()[0]
    print(f"land_use_summary table has {count} rows")
    
    # Get available scenarios
    scenarios = conn.execute("SELECT scenario_id, scenario_name FROM scenarios").fetchall()
    print(f"Available scenarios: {scenarios}")
    
    # Get available years
    years = conn.execute("SELECT DISTINCT year FROM land_use_summary ORDER BY year").fetchall()
    print(f"Available years: {years}")
    
    # Check the counties table structure
    print("\nExamining counties table structure:")
    counties_columns = conn.execute("PRAGMA table_info(counties)").fetchall()
    for col in counties_columns:
        print(f"Column: {col}")
    
    # Check if state_fips column exists in counties table
    has_state_fips = any(col[1] == 'state_fips' for col in counties_columns)
    print(f"Counties table has state_fips column: {has_state_fips}")
    
    # If state_fips column doesn't exist, check if we need to extract it from fips_code
    if not has_state_fips:
        print("Checking if fips_code can be used to derive state_fips:")
        county_samples = conn.execute("SELECT fips_code FROM counties LIMIT 5").fetchall()
        print(f"Sample county FIPS: {county_samples}")
        
        # Check if counties can be retrieved by state
        first_state = conn.execute("SELECT state_fips FROM states LIMIT 1").fetchone()[0]
        print(f"\nTrying to get counties for state FIPS: {first_state}")
        
        # Try to get counties by substring of fips_code (first 2 characters)
        counties_query = """
        SELECT fips_code, county_name 
        FROM counties 
        WHERE SUBSTRING(fips_code, 1, 2) = ? 
        LIMIT 5
        """
        counties_result = conn.execute(counties_query, [first_state]).fetchall()
        print(f"Counties found for state {first_state}: {len(counties_result)}")
        print(f"Sample counties: {counties_result}")
    
    # Try the first scenario and year combination
    if scenarios and years:
        first_scenario = scenarios[0][0]
        first_year = years[0][0]
        
        print(f"\nTrying query with scenario_id={first_scenario}, year={first_year}")
        
        query = """
        SELECT * FROM land_use_summary
        WHERE scenario_id = ? AND year = ?
        """
        
        result = conn.execute(query, [first_scenario, first_year]).fetchall()
        print(f"Query returned {len(result)} rows")
        
        if result:
            # Print column names and first row
            columns = conn.execute("SELECT * FROM land_use_summary LIMIT 0").description
            column_names = [column[0] for column in columns]
            print("\nColumns:", column_names)
            print("\nFirst row:", result[0])
        else:
            print(f"No data found for scenario_id={first_scenario}, year={first_year}")
            
            # Check all combinations
            print("\nChecking all scenario/year combinations with data:")
            combinations = conn.execute("""
                SELECT scenario_id, year, COUNT(*) 
                FROM land_use_summary 
                GROUP BY scenario_id, year 
                HAVING COUNT(*) > 0
                LIMIT 10
            """).fetchall()
            
            for combo in combinations:
                print(f"scenario_id={combo[0]}, year={combo[1]}, count={combo[2]}")
    
except Exception as e:
    print(f"Error: {e}")

finally:
    conn.close() 