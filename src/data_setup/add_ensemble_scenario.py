#!/usr/bin/env python3
"""
Add a new "ensemble" scenario to the RPA Land Use database.

This script:
1. Creates a new scenario record called "ensemble"
2. Calculates the mean land use transitions across all scenarios
3. Inserts the calculated transitions into the database with the new scenario ID
"""

import os
import sys
import logging
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.db.database import DBManager
from src.db.schema_manager import SchemaManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("ensemble-scenario-creator")

def check_if_ensemble_exists():
    """Check if the ensemble scenario already exists."""
    query = "SELECT scenario_id FROM scenarios WHERE scenario_name = 'ensemble'"
    result = DBManager.query_df(query)
    
    if not result.empty:
        logger.info(f"Ensemble scenario already exists with ID: {result['scenario_id'].iloc[0]}")
        return result['scenario_id'].iloc[0]
    
    return None

def get_all_scenarios():
    """Get all existing scenarios from the database."""
    query = "SELECT scenario_id, scenario_name FROM scenarios"
    return DBManager.query_df(query)

def create_ensemble_scenario():
    """Create a new scenario record for the ensemble scenario."""
    # Get the next available scenario ID
    query = "SELECT MAX(scenario_id) + 1 AS next_id FROM scenarios"
    result = DBManager.query_df(query)
    ensemble_id = int(result['next_id'].iloc[0])  # Convert numpy.int32 to Python int
    
    # Insert the new scenario
    insert_query = """
    INSERT INTO scenarios (scenario_id, scenario_name, gcm, rcp, ssp, description)
    VALUES (?, 'ensemble', 'ensemble', 'ensemble', 'ensemble', 'Mean transition of all scenarios')
    """
    
    with DBManager.connection() as conn:
        conn.execute(insert_query, [ensemble_id])
    
    logger.info(f"Created new ensemble scenario with ID: {ensemble_id}")
    return ensemble_id

def get_all_time_steps():
    """Get all time steps from the database."""
    query = "SELECT time_step_id FROM time_steps ORDER BY time_step_id"
    return DBManager.query_df(query)['time_step_id'].tolist()

def calculate_and_insert_ensemble_transitions(ensemble_id, scenario_ids, batch_size=50000):
    """Calculate and insert the ensemble transitions."""
    logger.info("Calculating and inserting ensemble transitions")
    
    # Get all time steps
    time_steps = get_all_time_steps()
    
    # Get the current max transition ID to start incrementing from
    max_id_query = "SELECT MAX(transition_id) + 1 AS next_id FROM land_use_transitions"
    max_id_result = DBManager.query_df(max_id_query)
    next_transition_id = int(max_id_result['next_id'].iloc[0])  # Convert numpy.int32 to Python int
    
    total_inserted = 0
    
    # Process each time step separately to manage memory
    for time_step_id in tqdm(time_steps, desc="Processing time steps"):
        logger.info(f"Processing time step ID: {time_step_id}")
        
        # Query to calculate the mean transitions for this time step
        ensemble_query = f"""
        SELECT 
            time_step_id,
            fips_code,
            from_land_use,
            to_land_use,
            AVG(area_hundreds_acres) AS area_hundreds_acres
        FROM 
            land_use_transitions
        WHERE 
            scenario_id IN ({','.join([str(id) for id in scenario_ids])})
            AND time_step_id = {time_step_id}
        GROUP BY 
            time_step_id, fips_code, from_land_use, to_land_use
        """
        
        # Get the ensemble data for this time step
        ensemble_df = DBManager.query_df(ensemble_query)
        
        if ensemble_df.empty:
            logger.warning(f"No data found for time step {time_step_id}")
            continue
        
        # Add the ensemble scenario ID and transition IDs
        ensemble_df['scenario_id'] = ensemble_id
        ensemble_df['transition_id'] = range(next_transition_id, next_transition_id + len(ensemble_df))
        next_transition_id += len(ensemble_df)
        
        # Process in batches to avoid memory issues
        for start_idx in range(0, len(ensemble_df), batch_size):
            end_idx = min(start_idx + batch_size, len(ensemble_df))
            batch_df = ensemble_df.iloc[start_idx:end_idx]
            
            # Insert the batch
            with DBManager.connection() as conn:
                conn.register('ensemble_batch', batch_df)
                conn.execute("""
                    INSERT INTO land_use_transitions
                    SELECT 
                        transition_id, scenario_id, time_step_id, 
                        fips_code, from_land_use, to_land_use, area_hundreds_acres
                    FROM 
                        ensemble_batch
                """)
            
            total_inserted += len(batch_df)
            logger.info(f"Inserted batch of {len(batch_df)} rows. Total: {total_inserted}")
    
    logger.info(f"Successfully inserted {total_inserted} ensemble transitions")

def main():
    """Main function to create the ensemble scenario."""
    logger.info("Starting creation of ensemble scenario")
    
    # Check if ensemble scenario already exists
    existing_id = check_if_ensemble_exists()
    if existing_id is not None:
        response = input(f"Ensemble scenario already exists with ID {existing_id}. Delete and recreate? (y/n): ")
        if response.lower() != 'y':
            logger.info("Exiting without changes")
            return
        
        # Delete existing ensemble scenario data
        logger.info("Deleting existing ensemble scenario data")
        with DBManager.connection() as conn:
            conn.execute("DELETE FROM land_use_transitions WHERE scenario_id = ?", [existing_id])
            conn.execute("DELETE FROM scenarios WHERE scenario_id = ?", [existing_id])
    
    # Get all existing scenarios (excluding any existing ensemble)
    scenarios_df = get_all_scenarios()
    if existing_id is not None:
        scenarios_df = scenarios_df[scenarios_df['scenario_id'] != existing_id]
    
    scenario_ids = scenarios_df['scenario_id'].tolist()
    logger.info(f"Found {len(scenario_ids)} scenarios to average")
    
    # Create the new ensemble scenario
    ensemble_id = create_ensemble_scenario()
    
    # Calculate and insert the ensemble transitions
    calculate_and_insert_ensemble_transitions(ensemble_id, scenario_ids)
    
    # Optimize the database after import
    logger.info("Optimizing database")
    SchemaManager.optimize_database()
    
    logger.info(f"Ensemble scenario created successfully with ID: {ensemble_id}")
    logger.info("To view the ensemble scenario, use: python query_db.py transitions --scenario {ensemble_id} --timestep 1")

if __name__ == "__main__":
    main() 