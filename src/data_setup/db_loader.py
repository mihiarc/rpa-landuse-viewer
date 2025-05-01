"""
Simplified database loader for RPA land use data.
Loads data from Parquet into DuckDB database with basic error handling.
"""

import pandas as pd
import logging
from pathlib import Path
from tqdm import tqdm
import os
import sys
import duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'database_path': os.getenv('DB_PATH', 'data/database/rpa_landuse_duck.db')
}

def get_db_connection():
    """
    Create a database connection using config.
    
    Returns:
        duckdb.DuckDBPyConnection: Database connection
    """
    db_path = DB_CONFIG['database_path']
    
    # Create parent directory if it doesn't exist
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Connecting to DuckDB database at {db_path}...")
    
    try:
        conn = duckdb.connect(db_path)
        # Enable parallelism
        conn.execute("PRAGMA threads=4")
        return conn
    except Exception as err:
        logger.error(f"Error connecting to database: {err}")
        raise

def initialize_database():
    """Create database tables if they don't exist."""
    logger.info("Initializing database schema...")
    conn = get_db_connection()
    
    create_tables = [
        """
        CREATE TABLE IF NOT EXISTS scenarios (
          scenario_id INTEGER PRIMARY KEY,
          scenario_name VARCHAR UNIQUE NOT NULL,
          gcm VARCHAR NOT NULL,
          rcp VARCHAR NOT NULL,
          ssp VARCHAR NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS time_steps (
          time_step_id INTEGER PRIMARY KEY,
          start_year INTEGER NOT NULL,
          end_year INTEGER NOT NULL,
          UNIQUE(start_year, end_year)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS counties (
          fips_code VARCHAR PRIMARY KEY,
          county_name VARCHAR
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS land_use_transitions (
          transition_id INTEGER PRIMARY KEY,
          scenario_id INTEGER NOT NULL,
          time_step_id INTEGER NOT NULL,
          fips_code VARCHAR NOT NULL,
          from_land_use VARCHAR NOT NULL,
          to_land_use VARCHAR NOT NULL,
          acres DOUBLE NOT NULL,
          FOREIGN KEY (scenario_id) REFERENCES scenarios(scenario_id),
          FOREIGN KEY (time_step_id) REFERENCES time_steps(time_step_id),
          FOREIGN KEY (fips_code) REFERENCES counties(fips_code)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_land_use_transitions 
        ON land_use_transitions (scenario_id, time_step_id, fips_code)
        """
    ]
    
    try:
        for query in create_tables:
            conn.execute(query)
        conn.commit()
        logger.info("Database schema initialized successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise
    finally:
        conn.close()

def load_to_duckdb(parquet_file, batch_size=1000):
    """
    Load Parquet data into DuckDB database.
    
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
        # Initialize database schema
        initialize_database()
        
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
    if scenario_data:
        scenario_df = pd.DataFrame(scenario_data, columns=['scenario_name', 'gcm', 'rcp', 'ssp'])
        conn.execute("""
            INSERT OR REPLACE INTO scenarios (scenario_name, gcm, rcp, ssp)
            SELECT scenario_name, gcm, rcp, ssp FROM scenario_df
        """, {"scenario_df": scenario_df})
    
    logger.info(f"Inserted {len(scenario_data)} scenarios")
    return len(scenario_data)

def _insert_time_steps(conn, df):
    """Insert unique time steps into the time_steps table."""
    logger.info("Inserting time steps...")
    
    # Extract unique time steps
    time_steps = df[['from_year', 'to_year']].drop_duplicates().values.tolist()
    time_step_df = pd.DataFrame(time_steps, columns=['start_year', 'end_year'])
    
    # Insert time steps
    if not time_step_df.empty:
        conn.execute("""
            INSERT OR REPLACE INTO time_steps (start_year, end_year)
            SELECT start_year, end_year FROM time_step_df
        """, {"time_step_df": time_step_df})
    
    logger.info(f"Inserted {len(time_steps)} time steps")
    return len(time_steps)

def _insert_counties(conn, df):
    """Insert unique counties into the counties table."""
    logger.info("Inserting counties...")
    
    # Extract unique counties
    counties = df[['fips', 'county_name']].drop_duplicates()
    counties.columns = ['fips_code', 'county_name']
    
    # Insert counties
    if not counties.empty:
        conn.execute("""
            INSERT OR REPLACE INTO counties (fips_code, county_name)
            SELECT fips_code, county_name FROM counties
        """, {"counties": counties})
    
    logger.info(f"Inserted {len(counties)} counties")
    return len(counties)

def _insert_transitions(conn, df, batch_size):
    """Insert land use transitions in batches."""
    logger.info("Inserting land use transitions...")
    
    # Prepare the data
    # First, get the scenario IDs
    scenario_ids = conn.execute("""
        SELECT scenario_id, scenario_name FROM scenarios
    """).fetchdf().set_index('scenario_name')['scenario_id'].to_dict()
    
    # Get the time step IDs
    time_step_ids = conn.execute("""
        SELECT time_step_id, start_year, end_year FROM time_steps
    """).fetchdf()
    time_step_ids['key'] = time_step_ids['start_year'].astype(str) + '_' + time_step_ids['end_year'].astype(str)
    time_step_ids = time_step_ids.set_index('key')['time_step_id'].to_dict()
    
    # Process data in batches
    total_records = len(df)
    batch_count = (total_records + batch_size - 1) // batch_size
    records_inserted = 0
    
    for i in tqdm(range(batch_count), desc="Processing transitions"):
        batch_start = i * batch_size
        batch_end = min((i + 1) * batch_size, total_records)
        batch_df = df.iloc[batch_start:batch_end].copy()
        
        # Add scenario_id column
        batch_df['scenario_id'] = batch_df['Scenario'].map(scenario_ids)
        
        # Add time_step_id column
        batch_df['time_key'] = batch_df['from_year'].astype(str) + '_' + batch_df['to_year'].astype(str)
        batch_df['time_step_id'] = batch_df['time_key'].map(time_step_ids)
        
        # Rename columns to match database schema
        batch_df.rename(columns={
            'fips': 'fips_code',
            'from_name': 'from_land_use',
            'to_name': 'to_land_use',
            'diff_area': 'acres'
        }, inplace=True)
        
        # Select only the columns needed for the database
        insert_df = batch_df[['scenario_id', 'time_step_id', 'fips_code',
                              'from_land_use', 'to_land_use', 'acres']].copy()
        
        # DuckDB-native fast insert
        conn.execute("""
            INSERT INTO land_use_transitions 
            (scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres)
            SELECT scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres 
            FROM insert_df
        """, {"insert_df": insert_df})
        
        # Update counter
        records_inserted += len(insert_df)
    
    logger.info(f"Inserted {records_inserted} land use transitions")
    return records_inserted

def verify_database_load():
    """Verify that data was loaded correctly and generate basic statistics."""
    logger.info("Verifying database load...")
    conn = get_db_connection()
    
    try:
        # Check for tables
        tables = conn.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'main'
        """).fetchall()
        logger.info(f"Found {len(tables)} tables in database")
        
        # Count records in each table
        scenarios_count = conn.execute("SELECT COUNT(*) FROM scenarios").fetchall()[0][0]
        time_steps_count = conn.execute("SELECT COUNT(*) FROM time_steps").fetchall()[0][0]
        counties_count = conn.execute("SELECT COUNT(*) FROM counties").fetchall()[0][0]
        transitions_count = conn.execute("SELECT COUNT(*) FROM land_use_transitions").fetchall()[0][0]
        
        # Log counts
        logger.info(f"Scenarios: {scenarios_count}")
        logger.info(f"Time Steps: {time_steps_count}")
        logger.info(f"Counties: {counties_count}")
        logger.info(f"Land Use Transitions: {transitions_count}")
        
        # Sample data from transitions
        sample = conn.execute("""
            SELECT * FROM land_use_transitions LIMIT 5
        """).fetchall()
        logger.info(f"Sample data from land_use_transitions: {sample}")
        
        return {
            "success": True,
            "table_count": len(tables),
            "scenarios_count": scenarios_count,
            "time_steps_count": time_steps_count,
            "counties_count": counties_count,
            "transitions_count": transitions_count
        }
    except Exception as e:
        logger.error(f"Error verifying database: {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def main():
    """Main function to load data into database."""
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <parquet_file>")
        sys.exit(1)
    
    parquet_file = sys.argv[1]
    print(f"Loading data from {parquet_file} into DuckDB database...")
    result = load_to_duckdb(parquet_file)
    
    if result["success"]:
        print("Data loaded successfully!")
        print(f"Records processed: {result['record_count']}")
        print(f"Scenarios inserted: {result['scenarios_inserted']}")
        print(f"Time steps inserted: {result['time_steps_inserted']}")
        print(f"Counties inserted: {result['counties_inserted']}")
        print(f"Transitions inserted: {result['transitions_inserted']}")
        
        # Verify the load
        verify_result = verify_database_load()
        if not verify_result["success"]:
            print(f"Warning: Database verification failed: {verify_result.get('error')}")
    else:
        print(f"Error loading data: {result.get('error')}")
        sys.exit(1)

if __name__ == "__main__":
    main() 