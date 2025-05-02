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
import argparse

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

# The following functions are for creating and managing views
def check_prerequisites(conn):
    """Check if all prerequisite tables exist and have data."""
    prerequisites = {
        "land_use_transitions": "Contains the core transition data",
        "scenarios": "Contains scenario information",
        "time_steps": "Contains time step information",
        "counties": "Contains county information",
        "states": "Contains state information",
        "rpa_regions": "Contains RPA region information",
        "rpa_subregions": "Contains RPA subregion information",
        "rpa_state_mapping": "Maps states to RPA subregions"
    }
    
    missing_tables = []
    empty_tables = []
    
    for table, description in prerequisites.items():
        try:
            # Check if table exists
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            logger.info(f"Table {table} exists with {count} rows")
            
            if count == 0:
                empty_tables.append((table, description))
        except Exception as e:
            logger.error(f"Table {table} does not exist: {e}")
            missing_tables.append((table, description))
    
    return missing_tables, empty_tables

def create_states_table_if_needed(conn):
    """Create and populate the states table if it doesn't exist."""
    try:
        # Check if table exists and has data
        count = conn.execute("SELECT COUNT(*) FROM states").fetchone()[0]
        if count > 0:
            logger.info(f"States table already exists with {count} rows")
            return True
    except:
        # Table doesn't exist, create it
        logger.info("Creating states table...")
        
        # Create the table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS states (
                state_fips VARCHAR PRIMARY KEY,
                state_name VARCHAR,
                state_abbr VARCHAR
            )
        """)
        
        # Load state mapping data
        state_fips = {
            'Alabama': '01', 'Alaska': '02', 'Arizona': '04', 'Arkansas': '05', 'California': '06',
            'Colorado': '08', 'Connecticut': '09', 'Delaware': '10', 'Florida': '12', 'Georgia': '13',
            'Hawaii': '15', 'Idaho': '16', 'Illinois': '17', 'Indiana': '18', 'Iowa': '19',
            'Kansas': '20', 'Kentucky': '21', 'Louisiana': '22', 'Maine': '23', 'Maryland': '24',
            'Massachusetts': '25', 'Michigan': '26', 'Minnesota': '27', 'Mississippi': '28', 'Missouri': '29',
            'Montana': '30', 'Nebraska': '31', 'Nevada': '32', 'New Hampshire': '33', 'New Jersey': '34',
            'New Mexico': '35', 'New York': '36', 'North Carolina': '37', 'North Dakota': '38', 'Ohio': '39',
            'Oklahoma': '40', 'Oregon': '41', 'Pennsylvania': '42', 'Rhode Island': '44', 'South Carolina': '45',
            'South Dakota': '46', 'Tennessee': '47', 'Texas': '48', 'Utah': '49', 'Vermont': '50',
            'Virginia': '51', 'Washington': '53', 'West Virginia': '54', 'Wisconsin': '55', 'Wyoming': '56'
        }
        
        state_abbrs = {
            'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
            'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
            'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
            'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
            'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO',
            'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
            'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
            'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
            'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
            'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'
        }
        
        # Insert all states
        for state_name, fips in state_fips.items():
            abbr = state_abbrs.get(state_name, '')
            conn.execute(
                "INSERT INTO states (state_fips, state_name, state_abbr) VALUES (?, ?, ?)",
                (fips, state_name, abbr)
            )
        
        conn.commit()
        logger.info(f"Inserted {len(state_fips)} states into the database")
    
    return True

def create_county_region_map_view(conn):
    """Create a view that maps counties to regions and subregions."""
    try:
        conn.execute("""
        CREATE OR REPLACE VIEW county_region_map AS
        SELECT 
            c.fips_code,
            c.county_name,
            s.state_fips,
            s.state_name,
            s.state_abbr,
            rs.subregion_id,
            rs.subregion_name,
            r.region_id,
            r.region_name
        FROM 
            counties c
        JOIN 
            states s ON SUBSTR(c.fips_code, 1, 2) = s.state_fips
        JOIN 
            rpa_state_mapping rsm ON s.state_fips = rsm.state_fips
        JOIN 
            rpa_subregions rs ON rsm.subregion_id = rs.subregion_id
        JOIN 
            rpa_regions r ON rs.parent_region_id = r.region_id
        """)
        logger.info("Created county_region_map view")
        return True
    except Exception as e:
        logger.error(f"Error creating county_region_map view: {e}")
        return False

def create_state_region_map_view(conn):
    """Create a view that maps states to regions and subregions."""
    try:
        conn.execute("""
        CREATE OR REPLACE VIEW state_region_map AS
        SELECT 
            s.state_fips,
            s.state_name,
            s.state_abbr,
            rs.subregion_id,
            rs.subregion_name,
            r.region_id,
            r.region_name
        FROM 
            states s
        JOIN 
            rpa_state_mapping rsm ON s.state_fips = rsm.state_fips
        JOIN 
            rpa_subregions rs ON rsm.subregion_id = rs.subregion_id
        JOIN 
            rpa_regions r ON rs.parent_region_id = r.region_id
        """)
        logger.info("Created state_region_map view")
        return True
    except Exception as e:
        logger.error(f"Error creating state_region_map view: {e}")
        return False

def create_region_land_use_view(conn):
    """Create a view for land use by region."""
    try:
        conn.execute("""
        CREATE OR REPLACE VIEW rpa_region_land_use AS
        SELECT 
            lut.scenario_id,
            s.scenario_name,
            ts.start_year,
            ts.end_year,
            crm.region_id AS rpa_region_id,
            crm.region_name AS rpa_region_name,
            lut.from_land_use,
            lut.to_land_use,
            SUM(lut.acres) AS acres
        FROM 
            land_use_transitions lut
        JOIN 
            scenarios s ON lut.scenario_id = s.scenario_id
        JOIN 
            time_steps ts ON lut.time_step_id = ts.time_step_id
        JOIN 
            county_region_map crm ON lut.fips_code = crm.fips_code
        GROUP BY 
            lut.scenario_id, s.scenario_name, ts.start_year, ts.end_year,
            crm.region_id, crm.region_name, lut.from_land_use, lut.to_land_use
        """)
        logger.info("Created rpa_region_land_use view")
        return True
    except Exception as e:
        logger.error(f"Error creating rpa_region_land_use view: {e}")
        return False

def create_subregion_land_use_view(conn):
    """Create a view for land use by subregion."""
    try:
        conn.execute("""
        CREATE OR REPLACE VIEW rpa_subregion_land_use AS
        SELECT 
            lut.scenario_id,
            s.scenario_name,
            ts.start_year,
            ts.end_year,
            crm.subregion_id,
            crm.subregion_name,
            crm.region_id AS parent_region_id,
            crm.region_name AS parent_region_name,
            lut.from_land_use,
            lut.to_land_use,
            SUM(lut.acres) AS acres
        FROM 
            land_use_transitions lut
        JOIN 
            scenarios s ON lut.scenario_id = s.scenario_id
        JOIN 
            time_steps ts ON lut.time_step_id = ts.time_step_id
        JOIN 
            county_region_map crm ON lut.fips_code = crm.fips_code
        GROUP BY 
            lut.scenario_id, s.scenario_name, ts.start_year, ts.end_year,
            crm.subregion_id, crm.subregion_name, crm.region_id, crm.region_name,
            lut.from_land_use, lut.to_land_use
        """)
        logger.info("Created rpa_subregion_land_use view")
        return True
    except Exception as e:
        logger.error(f"Error creating rpa_subregion_land_use view: {e}")
        return False

def create_state_land_use_view(conn):
    """Create a view for land use by state."""
    try:
        conn.execute("""
        CREATE OR REPLACE VIEW rpa_state_land_use AS
        SELECT 
            lut.scenario_id,
            s.scenario_name,
            ts.start_year,
            ts.end_year,
            st.state_fips,
            st.state_name,
            st.state_abbr,
            crm.subregion_id,
            crm.subregion_name,
            crm.region_id,
            crm.region_name,
            lut.from_land_use,
            lut.to_land_use,
            SUM(lut.acres) AS acres
        FROM 
            land_use_transitions lut
        JOIN 
            scenarios s ON lut.scenario_id = s.scenario_id
        JOIN 
            time_steps ts ON lut.time_step_id = ts.time_step_id
        JOIN 
            counties c ON lut.fips_code = c.fips_code
        JOIN 
            county_region_map crm ON c.fips_code = crm.fips_code
        JOIN
            states st ON crm.state_fips = st.state_fips
        GROUP BY 
            lut.scenario_id, s.scenario_name, ts.start_year, ts.end_year,
            st.state_fips, st.state_name, st.state_abbr,
            crm.subregion_id, crm.subregion_name, crm.region_id, crm.region_name,
            lut.from_land_use, lut.to_land_use
        """)
        logger.info("Created rpa_state_land_use view")
        return True
    except Exception as e:
        logger.error(f"Error creating rpa_state_land_use view: {e}")
        return False

def verify_views(conn):
    """Verify that all views were created properly."""
    views_to_check = [
        "county_region_map",
        "state_region_map",
        "rpa_region_land_use",
        "rpa_subregion_land_use",
        "rpa_state_land_use"
    ]
    
    success = True
    for view in views_to_check:
        try:
            # Simple query to check if view exists and works
            result = conn.execute(f"SELECT COUNT(*) FROM {view}").fetchone()
            count = result[0] if result else 0
            logger.info(f"View {view} verified with {count} rows")
        except Exception as e:
            logger.error(f"View {view} verification failed: {e}")
            success = False
    
    return success

def create_views():
    """Create all required views for region analysis."""
    logger.info("Creating views for regional analysis...")
    
    try:
        # Connect to the database
        conn = get_db_connection()
        
        # Check prerequisites
        missing_tables, empty_tables = check_prerequisites(conn)
        
        if missing_tables:
            logger.warning("Missing required tables:")
            for table, desc in missing_tables:
                logger.warning(f"  - {table}: {desc}")
            
            if "states" in [t[0] for t in missing_tables]:
                logger.info("Attempting to create states table...")
                create_states_table_if_needed(conn)
            
            if len(missing_tables) > 1 or "states" not in [t[0] for t in missing_tables]:
                logger.error("Cannot proceed with view creation due to missing tables")
                return False
        
        if empty_tables:
            logger.warning("The following tables have no data:")
            for table, desc in empty_tables:
                logger.warning(f"  - {table}: {desc}")
        
        # Create basic mapping views
        logger.info("Creating basic mapping views...")
        if not create_county_region_map_view(conn):
            logger.error("Failed to create county region map view")
            return False
        
        if not create_state_region_map_view(conn):
            logger.error("Failed to create state region map view")
            return False
        
        # Create the land use views
        logger.info("Creating land use views...")
        if not create_region_land_use_view(conn):
            logger.error("Failed to create region land use view")
            return False
        
        if not create_subregion_land_use_view(conn):
            logger.error("Failed to create subregion land use view")
            return False
        
        if not create_state_land_use_view(conn):
            logger.error("Failed to create state land use view")
            return False
        
        # Verify all views
        logger.info("Verifying views...")
        if not verify_views(conn):
            logger.error("View verification failed")
            return False
        
        logger.info("All views created and verified successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error creating views: {e}")
        return False
    finally:
        if 'conn' in locals() and conn:
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
        
        # Create views for regional analysis
        if create_views():
            stats["views_created"] = True
            logger.info("Regional views created successfully")
        else:
            stats["views_created"] = False
            logger.warning("Failed to create regional views")
        
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
    """
    Command-line interface for the database loader.
    Usage: python -m src.data_setup.db_loader <parquet_file> [--views-only]
    """
    parser = argparse.ArgumentParser(description="Load RPA land use data into DuckDB")
    parser.add_argument("parquet_file", nargs="?", help="Path to Parquet file to load", default=None)
    parser.add_argument("--views-only", action="store_true", help="Only create views without loading data")
    
    args = parser.parse_args()
    
    if args.views_only:
        # Create views without loading data
        logger.info("Creating views without loading data...")
        success = create_views()
        return 0 if success else 1
    
    if not args.parquet_file:
        parser.print_help()
        return 1
    
    # Load data and create views
    result = load_to_duckdb(args.parquet_file)
    if result["success"]:
        logger.info("Data loading completed successfully")
        return 0
    else:
        logger.error(f"Data loading failed: {result.get('error', 'Unknown error')}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 