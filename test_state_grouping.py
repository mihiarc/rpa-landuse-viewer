#!/usr/bin/env python
"""
Diagnostic script to test state grouping functionality in the RPA Land Use database.
This script performs various queries directly against the database to verify the 
correctness of state data, relationships, and aggregation.
"""

import sqlite3
import pandas as pd
import sys
import os
from pathlib import Path

# Database path
DB_PATH = 'data/database/rpa_landuse.db'

def print_section(title):
    """Print a section header"""
    print("\n" + "="*80)
    print(f" {title} ".center(80, '='))
    print("="*80)

def run_query(query, params=None, title=None):
    """Run a query and print results with an optional title"""
    if title:
        print(f"\n--- {title} ---")
    
    conn = sqlite3.connect(DB_PATH)
    
    try:
        if params:
            df = pd.read_sql_query(query, conn, params=params)
        else:
            df = pd.read_sql_query(query, conn)
        
        if not df.empty:
            print(f"Results: {len(df)} rows")
            print(df)
        else:
            print("No results found")
        
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    finally:
        conn.close()

def test_state_table():
    """Test if states table exists and contains data"""
    print_section("TESTING STATE TABLE")
    
    # Check if states table exists
    query = """
    SELECT name FROM sqlite_master 
    WHERE type='table' AND name='states'
    """
    run_query(query, title="Verify states table exists")
    
    # Count states
    query = "SELECT COUNT(*) AS state_count FROM states"
    run_query(query, title="Count states")
    
    # Sample state data
    query = """
    SELECT state_fips, state_name, state_abbr 
    FROM states 
    LIMIT 10
    """
    run_query(query, title="Sample state data")

def test_county_state_relationship():
    """Test relationship between counties and states"""
    print_section("TESTING COUNTY-STATE RELATIONSHIP")
    
    # Check county_state_map view
    query = """
    SELECT name FROM sqlite_master 
    WHERE type='view' AND name='county_state_map'
    """
    run_query(query, title="Verify county_state_map view exists")
    
    # Sample county-state mappings
    query = """
    SELECT county_fips, county_name, state_fips, state_name, state_abbr
    FROM county_state_map
    LIMIT 10
    """
    run_query(query, title="Sample county-state mappings")
    
    # Count counties per state (top 10 states)
    query = """
    SELECT state_name, state_abbr, COUNT(*) AS county_count
    FROM county_state_map
    GROUP BY state_fips
    ORDER BY county_count DESC
    LIMIT 10
    """
    run_query(query, title="Counties per state (top 10)")
    
    # Check for counties without state mappings
    query = """
    SELECT c.fips_code, c.county_name
    FROM counties c
    LEFT JOIN states s ON SUBSTR(c.fips_code, 1, 2) = s.state_fips
    WHERE s.state_fips IS NULL
    LIMIT 20
    """
    run_query(query, title="Counties without state mappings")

def test_state_aggregation():
    """Test state-level aggregation of land use data"""
    print_section("TESTING STATE-LEVEL AGGREGATION")
    
    # Check state_land_use_transitions view
    query = """
    SELECT name FROM sqlite_master 
    WHERE type='view' AND name='state_land_use_transitions'
    """
    run_query(query, title="Verify state_land_use_transitions view exists")
    
    # Sample state-level aggregated data
    query = """
    SELECT scenario_id, time_step_id, state_fips, state_name, 
           from_land_use, to_land_use, acres
    FROM state_land_use_transitions
    LIMIT 10
    """
    run_query(query, title="Sample state-level land use transitions")
    
    # Check a specific state (e.g., California = '06')
    query = """
    SELECT scenario_id, time_step_id, state_name, 
           from_land_use, to_land_use, acres
    FROM state_land_use_transitions
    WHERE state_fips = '06'
    LIMIT 10
    """
    run_query(query, title="California land use transitions")
    
    # Sum acres by state for a scenario
    query = """
    SELECT state_name, SUM(acres) AS total_acres
    FROM state_land_use_transitions
    WHERE scenario_id = 1
    GROUP BY state_fips
    ORDER BY total_acres DESC
    LIMIT 10
    """
    run_query(query, title="Top 10 states by total acres (scenario 1)")

def test_region_hierarchy():
    """Test region hierarchy view"""
    print_section("TESTING REGION HIERARCHY")
    
    # Check region_hierarchy view
    query = """
    SELECT name FROM sqlite_master 
    WHERE type='view' AND name='region_hierarchy'
    """
    run_query(query, title="Verify region_hierarchy view exists")
    
    # Sample region hierarchy data
    query = """
    SELECT region_id, region_name, region_type, parent_id, level
    FROM region_hierarchy
    LIMIT 15
    """
    run_query(query, title="Sample region hierarchy data")
    
    # Test retrieving all counties in a state using the hierarchy
    query = """
    SELECT region_id, region_name, region_type, parent_id
    FROM region_hierarchy
    WHERE parent_id = '06' AND region_type = 'COUNTY'
    LIMIT 10
    """
    run_query(query, title="Counties in California using hierarchy")

def test_land_use_by_state():
    """Test land use data aggregation by state"""
    print_section("TESTING LAND USE DATA BY STATE")
    
    # Test retrieving aggregated land use data for a state
    query = """
    WITH state_transitions_summary AS (
        SELECT 
            lut.scenario_id,
            SUBSTR(lut.fips_code, 1, 2) as state_fips,
            st.state_name,
            lut.from_land_use as land_use_type,
            -SUM(lut.acres) as net_change
        FROM land_use_transitions lut
        JOIN states st ON SUBSTR(lut.fips_code, 1, 2) = st.state_fips
        WHERE lut.scenario_id = 1
        GROUP BY lut.scenario_id, state_fips, st.state_name, lut.from_land_use
        
        UNION ALL
        
        SELECT 
            lut.scenario_id,
            SUBSTR(lut.fips_code, 1, 2) as state_fips,
            st.state_name,
            lut.to_land_use as land_use_type,
            SUM(lut.acres) as net_change
        FROM land_use_transitions lut
        JOIN states st ON SUBSTR(lut.fips_code, 1, 2) = st.state_fips
        WHERE lut.scenario_id = 1
        GROUP BY lut.scenario_id, state_fips, st.state_name, lut.to_land_use
    )
    
    SELECT 
        state_fips,
        state_name,
        land_use_type,
        SUM(net_change) as total_acres
    FROM state_transitions_summary
    WHERE state_fips = '06'  -- California
    GROUP BY state_fips, state_name, land_use_type
    ORDER BY total_acres DESC
    """
    run_query(query, title="Land use by type in California (Scenario 1)")
    
    # Count states with transitions
    query = """
    SELECT COUNT(DISTINCT SUBSTR(fips_code, 1, 2)) AS states_with_data
    FROM land_use_transitions
    """
    run_query(query, title="Number of states with transition data")
    
    # Verify state_fips mapping in direct query
    query = """
    SELECT DISTINCT SUBSTR(fips_code, 1, 2) AS extracted_state_fips,
           s.state_fips, s.state_name
    FROM land_use_transitions lut
    LEFT JOIN states s ON SUBSTR(lut.fips_code, 1, 2) = s.state_fips
    LIMIT 20
    """
    run_query(query, title="State FIPS mapping verification")

def test_specific_state_filter():
    """Test filtering data for a specific state"""
    print_section("TESTING SPECIFIC STATE FILTERS")
    
    # Test for a specific state (California)
    state_fips = '06'
    
    # Get state details
    query = """
    SELECT state_fips, state_name, state_abbr
    FROM states
    WHERE state_fips = ?
    """
    state_info = run_query(query, params=(state_fips,), title=f"State info for FIPS {state_fips}")
    
    if state_info is not None and not state_info.empty:
        state_name = state_info.iloc[0]['state_name']
        
        # Count counties in this state
        query = """
        SELECT COUNT(*) AS county_count
        FROM counties
        WHERE SUBSTR(fips_code, 1, 2) = ?
        """
        run_query(query, params=(state_fips,), title=f"Number of counties in {state_name}")
        
        # Get sample counties in this state
        query = """
        SELECT fips_code, county_name
        FROM counties
        WHERE SUBSTR(fips_code, 1, 2) = ?
        LIMIT 10
        """
        run_query(query, params=(state_fips,), title=f"Sample counties in {state_name}")
        
        # Count transitions in this state
        query = """
        SELECT COUNT(*) AS transition_count
        FROM land_use_transitions
        WHERE SUBSTR(fips_code, 1, 2) = ?
        """
        run_query(query, params=(state_fips,), title=f"Number of transitions in {state_name}")
        
        # Test the get_national_summary function's SQL (state level)
        scenario_id = 1
        query = """
        WITH state_transitions_summary AS (
            SELECT 
                lut.scenario_id,
                CASE 
                    WHEN ts.end_year = 2020 THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                SUBSTR(lut.fips_code, 1, 2) as state_fips,
                st.state_name,
                st.state_abbr,
                lut.from_land_use as land_use_type,
                -SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            JOIN states st ON SUBSTR(lut.fips_code, 1, 2) = st.state_fips
            WHERE lut.scenario_id = ?
            AND SUBSTR(lut.fips_code, 1, 2) = ?
            GROUP BY lut.scenario_id, year, state_fips, st.state_name, st.state_abbr, lut.from_land_use
            
            UNION ALL
            
            SELECT 
                lut.scenario_id,
                CASE 
                    WHEN ts.end_year = 2020 THEN ts.end_year
                    ELSE ts.start_year
                END as year,
                SUBSTR(lut.fips_code, 1, 2) as state_fips,
                st.state_name,
                st.state_abbr,
                lut.to_land_use as land_use_type,
                SUM(lut.acres) as net_change
            FROM land_use_transitions lut
            JOIN scenarios s ON lut.scenario_id = s.scenario_id
            JOIN time_steps ts ON lut.time_step_id = ts.time_step_id
            JOIN states st ON SUBSTR(lut.fips_code, 1, 2) = st.state_fips
            WHERE lut.scenario_id = ?
            AND SUBSTR(lut.fips_code, 1, 2) = ?
            GROUP BY lut.scenario_id, year, state_fips, st.state_name, st.state_abbr, lut.to_land_use
        )
        
        SELECT 
            state_fips,
            state_name,
            year,
            land_use_type,
            SUM(net_change) as total_acres
        FROM state_transitions_summary
        GROUP BY state_fips, state_name, year, land_use_type
        ORDER BY year, land_use_type
        """
        params = (scenario_id, state_fips, scenario_id, state_fips)
        run_query(query, params=params, title=f"get_national_summary test for {state_name} (state level)")

if __name__ == "__main__":
    print(f"Testing state grouping functionality with database: {DB_PATH}")
    
    # Check if the database exists
    if not Path(DB_PATH).exists():
        print(f"Error: Database file not found at {DB_PATH}")
        sys.exit(1)
    
    # Run tests
    test_state_table()
    test_county_state_relationship()
    test_state_aggregation()
    test_region_hierarchy()
    test_land_use_by_state()
    test_specific_state_filter()
    
    print("\nAll tests completed.") 