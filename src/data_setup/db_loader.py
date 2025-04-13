"""
Simplified database loader for RPA land use data.
Loads data from Parquet into MySQL database with basic error handling.
"""

import pandas as pd
import mysql.connector
import logging
from pathlib import Path
from tqdm import tqdm
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """
    Create a database connection using environment variables or defaults.
    
    Returns:
        mysql.connector.connection: Database connection
    """
    host = os.environ.get('MYSQL_HOST', 'localhost')
    user = os.environ.get('MYSQL_USER', 'mihiarc')
    password = os.environ.get('MYSQL_PASSWORD', 'survista683')
    database = os.environ.get('MYSQL_DATABASE', 'rpa_mysql_db')
    
    logger.info(f"Connecting to MySQL database {database} on {host}...")
    
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        return conn
    except mysql.connector.Error as err:
        logger.error(f"Error connecting to database: {err}")
        raise

def load_to_mysql(parquet_file, batch_size=1000):
    """
    Load Parquet data into MySQL database.
    
    Args:
        parquet_file (str): Path to Parquet file
        batch_size (int): Batch size for inserting records
        
    Returns:
        dict: Loading statistics
    """
    if not Path(parquet_file).exists():
        logger.error(f"File not found: {parquet_file}")
        return {"success": False, "error": "File not found"}
    
    try:
        # Load data
        logger.info(f"Loading data from {parquet_file}...")
        df = pd.read_parquet(parquet_file)
        
        # Connect to database
        conn = get_db_connection()
        
        # Track statistics
        stats = {
            "success": True,
            "record_count": len(df),
            "scenarios_inserted": 0,
            "time_steps_inserted": 0,
            "counties_inserted": 0,
            "transitions_inserted": 0
        }
        
        # Insert data into tables
        logger.info("Loading data into database tables...")
        
        # Insert scenarios
        stats["scenarios_inserted"] = _insert_scenarios(conn, df)
        
        # Insert time steps
        stats["time_steps_inserted"] = _insert_time_steps(conn, df)
        
        # Insert counties
        stats["counties_inserted"] = _insert_counties(conn, df)
        
        # Insert transitions
        stats["transitions_inserted"] = _insert_transitions(conn, df, batch_size)
        
        # Close connection
        conn.close()
        
        logger.info("Database loading completed successfully!")
        return stats
    
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        if 'conn' in locals():
            conn.close()
        return {"success": False, "error": str(e)}

def _insert_scenarios(conn, df):
    """Insert unique scenarios into the scenarios table."""
    logger.info("Inserting scenarios...")
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
    cursor.close()
    
    logger.info(f"Inserted {len(scenario_data)} scenarios")
    return len(scenario_data)

def _insert_time_steps(conn, df):
    """Insert unique time steps into the time_steps table."""
    logger.info("Inserting time steps...")
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
    cursor.close()
    
    logger.info(f"Inserted {len(time_step_data)} time steps")
    return len(time_step_data)

def _insert_counties(conn, df):
    """Insert unique counties into the counties table."""
    logger.info("Inserting counties...")
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
    cursor.close()
    
    logger.info(f"Inserted {len(county_data)} counties")
    return len(county_data)

def _insert_transitions(conn, df, batch_size):
    """Insert land use transitions."""
    logger.info("Inserting land use transitions...")
    cursor = conn.cursor()
    
    # Get scenario and time step mappings
    cursor.execute("SELECT scenario_id, scenario_name FROM scenarios")
    scenario_map = {v: k for k, v in cursor.fetchall()}
    
    cursor.execute("SELECT time_step_id, CONCAT(start_year, '-', end_year) FROM time_steps")
    time_step_map = {v: k for k, v in cursor.fetchall()}
    
    # Insert transitions in batches with progress bar
    total_records = len(df)
    inserted_count = 0
    
    with tqdm(total=total_records) as pbar:
        batch = []
        
        for _, row in df.iterrows():
            batch.append((
                scenario_map[row['Scenario']],
                time_step_map[row['YearRange']],
                row['FIPS'],
                row['From'],
                row['To'],
                float(row['Acres'])
            ))
            
            # Insert when batch is full
            if len(batch) >= batch_size:
                query = """
                    INSERT INTO land_use_transitions 
                    (scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                cursor.executemany(query, batch)
                conn.commit()
                
                inserted_count += len(batch)
                pbar.update(len(batch))
                batch = []
        
        # Insert remaining records
        if batch:
            query = """
                INSERT INTO land_use_transitions 
                (scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(query, batch)
            conn.commit()
            
            inserted_count += len(batch)
            pbar.update(len(batch))
    
    cursor.close()
    logger.info(f"Inserted {inserted_count} land use transitions")
    return inserted_count

def verify_database_load():
    """
    Verify that data was loaded correctly into the database.
    
    Returns:
        dict: Verification results
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Check counts in each table
        tables = ['scenarios', 'time_steps', 'counties', 'land_use_transitions']
        counts = {}
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) AS count FROM {table}")
            result = cursor.fetchone()
            counts[table] = result['count']
        
        # Get a sample of data from land_use_transitions
        cursor.execute("""
            SELECT t.*, s.scenario_name, CONCAT(ts.start_year, '-', ts.end_year) AS year_range
            FROM land_use_transitions t
            JOIN scenarios s ON t.scenario_id = s.scenario_id
            JOIN time_steps ts ON t.time_step_id = ts.time_step_id
            LIMIT 5
        """)
        sample = cursor.fetchall()
        
        conn.close()
        
        return {
            "success": True,
            "table_counts": counts,
            "sample_data": sample
        }
    
    except Exception as e:
        logger.error(f"Error verifying database: {str(e)}")
        if 'conn' in locals():
            conn.close()
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import argparse
    import json
    
    parser = argparse.ArgumentParser(description='Load RPA land use data into MySQL.')
    parser.add_argument('--input', default='data/processed/rpa_landuse_data.parquet',
                      help='Input Parquet file path')
    parser.add_argument('--batch', type=int, default=1000,
                      help='Batch size for inserting records')
    parser.add_argument('--verify', action='store_true',
                      help='Verify database after loading')
    args = parser.parse_args()
    
    # Load data
    stats = load_to_mysql(args.input, args.batch)
    
    print("\nDatabase Loading Summary:")
    if stats["success"]:
        print("Status: Success")
        print(f"Records processed: {stats['record_count']:,}")
        print(f"Scenarios inserted: {stats['scenarios_inserted']}")
        print(f"Time steps inserted: {stats['time_steps_inserted']}")
        print(f"Counties inserted: {stats['counties_inserted']}")
        print(f"Transitions inserted: {stats['transitions_inserted']:,}")
    else:
        print(f"Status: Failed - {stats.get('error', 'Unknown error')}")
    
    # Verify if requested
    if args.verify:
        print("\nVerifying database...")
        verify_results = verify_database_load()
        
        if verify_results["success"]:
            print("Verification successful!")
            print("\nTable record counts:")
            for table, count in verify_results["table_counts"].items():
                print(f"- {table}: {count:,}")
        else:
            print(f"Verification failed: {verify_results.get('error', 'Unknown error')}") 