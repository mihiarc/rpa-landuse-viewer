"""Script to load parquet data into MySQL database."""

import pandas as pd
import mysql.connector
from pathlib import Path
import sys
from tqdm import tqdm

def get_db_connection():
    """Create database connection."""
    return mysql.connector.connect(
        host="localhost",
        user="mihiarc",
        password="survista683",
        database="rpa_mysql_db"
    )

def load_parquet_data(file_path="../../rpa_landuse_data.parquet"):
    """Load data from parquet file."""
    print(f"Loading data from {file_path}...")
    return pd.read_parquet(file_path)

def insert_scenarios(conn, df):
    """Insert unique scenarios into the scenarios table."""
    print("\nInserting scenarios...")
    cursor = conn.cursor()
    
    # Extract unique scenarios and their components
    scenarios = df['Scenario'].unique()
    scenario_data = []
    for scenario in scenarios:
        # Parse scenario name (format: GCM_RCP_SSP)
        parts = scenario.split('_')
        gcm = parts[0]
        rcp = parts[1].lower()
        ssp = parts[2].lower()
        scenario_data.append((scenario, gcm, rcp, ssp))
    
    # Insert scenarios
    query = """
        INSERT INTO scenarios (scenario_name, gcm, rcp, ssp)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        gcm=VALUES(gcm), rcp=VALUES(rcp), ssp=VALUES(ssp)
    """
    cursor.executemany(query, scenario_data)
    conn.commit()
    print(f"Inserted {len(scenario_data)} scenarios")

def insert_time_steps(conn, df):
    """Insert unique time steps into the time_steps table."""
    print("\nInserting time steps...")
    cursor = conn.cursor()
    
    # Extract unique year ranges
    year_ranges = df['YearRange'].unique()
    time_step_data = []
    for year_range in year_ranges:
        start_year, end_year = map(int, year_range.split('-'))
        time_step_data.append((start_year, end_year))
    
    # Insert time steps
    query = """
        INSERT INTO time_steps (start_year, end_year)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
        start_year=VALUES(start_year), end_year=VALUES(end_year)
    """
    cursor.executemany(query, time_step_data)
    conn.commit()
    print(f"Inserted {len(time_step_data)} time steps")

def insert_counties(conn, df):
    """Insert unique counties into the counties table."""
    print("\nInserting counties...")
    cursor = conn.cursor()
    
    # Extract unique FIPS codes
    counties = df['FIPS'].unique()
    county_data = [(fips, f"County {fips}") for fips in counties]
    
    # Insert counties
    query = """
        INSERT INTO counties (fips_code, county_name)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
        county_name=VALUES(county_name)
    """
    cursor.executemany(query, county_data)
    conn.commit()
    print(f"Inserted {len(county_data)} counties")

def insert_transitions(conn, df):
    """Insert land use transitions."""
    print("\nInserting land use transitions...")
    cursor = conn.cursor()
    
    # Get scenario and time step mappings
    cursor.execute("SELECT scenario_id, scenario_name FROM scenarios")
    scenario_map = dict(cursor.fetchall())
    scenario_map = {v: k for k, v in scenario_map.items()}
    
    cursor.execute("SELECT time_step_id, CONCAT(start_year, '-', end_year) FROM time_steps")
    time_step_map = dict(cursor.fetchall())
    time_step_map = {v: k for k, v in time_step_map.items()}
    
    # Prepare transition data
    transitions = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        transitions.append((
            scenario_map[row['Scenario']],
            time_step_map[row['YearRange']],
            row['FIPS'],
            row['From'],
            row['To'],
            float(row['Acres'])
        ))
        
        # Insert in batches of 1000
        if len(transitions) >= 1000:
            query = """
                INSERT INTO land_use_transitions 
                (scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(query, transitions)
            conn.commit()
            transitions = []
    
    # Insert remaining transitions
    if transitions:
        query = """
            INSERT INTO land_use_transitions 
            (scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(query, transitions)
        conn.commit()

def main():
    """Main function to load data into MySQL."""
    try:
        # Connect to database
        conn = get_db_connection()
        print("Connected to database successfully!")
        
        # Load parquet data
        df = load_parquet_data()
        print(f"Loaded {len(df)} records from parquet file")
        
        # Insert data into tables
        insert_scenarios(conn, df)
        insert_time_steps(conn, df)
        insert_counties(conn, df)
        insert_transitions(conn, df)
        
        print("\nData loading completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main() 