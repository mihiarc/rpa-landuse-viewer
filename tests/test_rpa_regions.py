#!/usr/bin/env python3
"""
Test script for RPA regions functionality.
"""

import sys
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.db.rpa_regions import RPARegions

def print_section(title):
    """Print a section header."""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)

def print_table(data, max_rows=10):
    """Print data as a formatted table."""
    if data is None:
        print("No data found.")
        return
    
    if isinstance(data, list):
        if not data:
            print("No data found (empty list).")
            return
        # Convert list of dicts to DataFrame for prettier printing
        df = pd.DataFrame(data)
        print(df.head(max_rows))
        if len(data) > max_rows:
            print(f"... and {len(data) - max_rows} more rows")
    elif isinstance(data, pd.DataFrame):
        if data.empty:
            print("No data found (empty DataFrame).")
            return
        print(data.head(max_rows))
        if len(data) > max_rows:
            print(f"... and {len(data) - max_rows} more rows")
    else:
        print(data)

def main():
    """Main test function."""
    print_section("RPA REGIONS TEST")
    
    # Test getting all regions
    print_section("All RPA Regions")
    regions = RPARegions.get_all_regions()
    print_table(regions)
    
    # Test getting subregions
    print_section("All RPA Subregions")
    subregions = RPARegions.get_subregions()
    print_table(subregions)
    
    # Test getting subregions for a specific region
    print_section("North Region Subregions")
    north_subregions = RPARegions.get_subregions('NORTH')
    print_table(north_subregions)
    
    # Test getting states by region
    print_section("States in North Region")
    north_states = RPARegions.get_states_by_region('NORTH')
    print_table(north_states)
    
    # Test getting states by subregion
    print_section("States in Northeast Subregion")
    northeast_states = RPARegions.get_states_by_region(subregion_id='NORTHEAST')
    print_table(northeast_states)
    
    # Test getting counties by region
    print_section("Sample Counties in Pacific Coast Region")
    pacific_counties = RPARegions.get_counties_by_region('PACIFIC')
    print_table(pacific_counties, max_rows=5)
    
    # Test getting counties by subregion
    print_section("Sample Counties in Alaska Subregion")
    alaska_counties = RPARegions.get_counties_by_region(subregion_id='ALASKA')
    print_table(alaska_counties, max_rows=5)
    
    # Get scenarios to use for land use queries
    print_section("Testing Land Use Data Aggregation")
    try:
        # Get the first scenario ID from the database
        from src.db.database import DatabaseConnection
        conn = DatabaseConnection.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT scenario_id FROM scenarios LIMIT 1")
        scenario_id = cursor.fetchone()[0]
        DatabaseConnection.close_connection(conn)
        
        # Test getting land use by region
        print(f"Using scenario_id: {scenario_id}")
        print_section("Sample Land Use Data by RPA Region")
        region_data = RPARegions.get_land_use_by_region(scenario_id)
        print_table(region_data, max_rows=5)
        
        # Test getting land use by subregion
        print_section("Sample Land Use Data by RPA Subregion")
        subregion_data = RPARegions.get_land_use_by_subregion(scenario_id)
        print_table(subregion_data, max_rows=5)
    except Exception as e:
        print(f"Error testing land use data: {e}")

if __name__ == "__main__":
    main() 