"""
Simplified database loader for RPA land use data.
Loads data from Parquet into SQLite database with basic error handling.
"""

import pandas as pd
import sqlite3
import logging
from pathlib import Path
from tqdm import tqdm
import os
import sys

# Add parent directory to path to allow importing from sibling modules
sys.path.append(str(Path(__file__).parent.parent))
from api.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """
    Create a database connection using config.
    
    Returns:
        sqlite3.Connection: Database connection
    """
    db_path = Config.get_db_config()['database_path']
    
    # Create parent directory if it doesn't exist
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Connecting to SQLite database at {db_path}...")
    
    try:
        conn = sqlite3.connect(db_path)
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    except sqlite3.Error as err:
        logger.error(f"Error connecting to database: {err}")
        raise

def initialize_database():
    """Create database tables if they don't exist."""
    logger.info("Initializing database schema...")
    conn = get_db_connection()
    
    create_tables = [
        """
        CREATE TABLE IF NOT EXISTS scenarios (
          scenario_id INTEGER PRIMARY KEY AUTOINCREMENT,
          scenario_name TEXT UNIQUE NOT NULL,
          gcm TEXT NOT NULL,
          rcp TEXT NOT NULL,
          ssp TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS time_steps (
          time_step_id INTEGER PRIMARY KEY AUTOINCREMENT,
          start_year INTEGER NOT NULL,
          end_year INTEGER NOT NULL,
          UNIQUE(start_year, end_year)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS counties (
          fips_code TEXT PRIMARY KEY,
          county_name TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS land_use_transitions (
          transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
          scenario_id INTEGER NOT NULL,
          time_step_id INTEGER NOT NULL,
          fips_code TEXT NOT NULL,
          from_land_use TEXT NOT NULL,
          to_land_use TEXT NOT NULL,
          acres REAL NOT NULL,
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
    except sqlite3.Error as e:
        conn.rollback()
        logger.error(f"Error creating tables: {e}")
        raise
    finally:
        conn.close()

def load_to_sqlite(parquet_file, batch_size=1000):
    """
    Load Parquet data into SQLite database.
    
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
        INSERT OR REPLACE INTO scenarios (scenario_name, gcm, rcp, ssp)
        VALUES (?, ?, ?, ?)
    """
    cursor.executemany(query, scenario_data)
    conn.commit()
    
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
        INSERT OR REPLACE INTO time_steps (start_year, end_year)
        VALUES (?, ?)
    """
    cursor.executemany(query, time_step_data)
    conn.commit()
    
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
        INSERT OR REPLACE INTO counties (fips_code, county_name)
        VALUES (?, ?)
    """
    cursor.executemany(query, county_data)
    conn.commit()
    
    logger.info(f"Inserted {len(county_data)} counties")
    return len(county_data)

def _insert_transitions(conn, df, batch_size):
    """Insert land use transitions."""
    logger.info("Inserting land use transitions...")
    cursor = conn.cursor()
    
    # Get scenario and time step mappings
    cursor.execute("SELECT scenario_id, scenario_name FROM scenarios")
    scenario_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT time_step_id, start_year || '-' || end_year FROM time_steps")
    time_step_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Insert transitions in batches with progress bar
    total_records = len(df)
    inserted_count = 0
    
    # Prepare batch inserts with progress bar
    query = """
        INSERT INTO land_use_transitions 
        (scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres)
        VALUES (?, ?, ?, ?, ?, ?)
    """
    
    with tqdm(total=total_records, desc="Loading transitions") as pbar:
        batch_data = []
        
        for _, row in df.iterrows():
            scenario_id = scenario_map[row['Scenario']]
            time_step_id = time_step_map[row['YearRange']]
            fips_code = row['FIPS']
            from_land_use = row['From']
            to_land_use = row['To']
            acres = row['Acres']
            
            batch_data.append((scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres))
            
            if len(batch_data) >= batch_size:
                try:
                    cursor.executemany(query, batch_data)
                    conn.commit()
                    inserted_count += len(batch_data)
                    pbar.update(len(batch_data))
                    batch_data = []
                except sqlite3.Error as e:
                    conn.rollback()
                    logger.error(f"Error inserting batch: {e}")
                    raise
        
        # Insert remaining records
        if batch_data:
            try:
                cursor.executemany(query, batch_data)
                conn.commit()
                inserted_count += len(batch_data)
                pbar.update(len(batch_data))
            except sqlite3.Error as e:
                conn.rollback()
                logger.error(f"Error inserting final batch: {e}")
                raise
    
    logger.info(f"Inserted {inserted_count} land use transitions")
    return inserted_count

def verify_database_load():
    """Verify database load by checking record counts."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check table record counts
        tables = ['scenarios', 'time_steps', 'counties', 'land_use_transitions']
        counts = {}
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            counts[table] = count
            logger.info(f"Table {table}: {count} records")
        
        # Check a sample query
        cursor.execute("""
            SELECT s.scenario_name, ts.start_year, ts.end_year, COUNT(*)
            FROM land_use_transitions t
            JOIN scenarios s ON t.scenario_id = s.scenario_id
            JOIN time_steps ts ON t.time_step_id = ts.time_step_id
            GROUP BY s.scenario_name, ts.start_year, ts.end_year
            LIMIT 5
        """)
        
        sample = cursor.fetchall()
        logger.info("Sample query result:")
        for row in sample:
            logger.info(f"  Scenario: {row[0]}, Period: {row[1]}-{row[2]}, Transitions: {row[3]}")
        
        return counts
        
    except sqlite3.Error as e:
        logger.error(f"Error verifying database: {e}")
        return None
    finally:
        conn.close()

if __name__ == "__main__":
    # Default parquet file path, can be overridden via command line
    default_parquet_path = str(Path(__file__).parent.parent.parent / "data" / "processed" / "rpa_landuse_data.parquet")
    parquet_file = sys.argv[1] if len(sys.argv) > 1 else default_parquet_path
    
    print(f"Loading data from {parquet_file} into SQLite database...")
    result = load_to_sqlite(parquet_file)
    
    if result["success"]:
        print("Data loaded successfully!")
        print(f"Scenarios: {result['scenarios_inserted']}")
        print(f"Time steps: {result['time_steps_inserted']}")
        print(f"Counties: {result['counties_inserted']}")
        print(f"Transitions: {result['transitions_inserted']}")
        
        # Verify the load
        verify_database_load()
    else:
        print(f"Error loading data: {result['error']}") 