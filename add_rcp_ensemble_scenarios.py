#!/usr/bin/env python3
"""
Add ensemble scenarios for RCPs to the RPA Land Use database.

This script:
1. Identifies all unique GCM+SSP combinations
2. For each combination, creates an ensemble scenario that averages across all RCPs
3. Inserts the calculated transitions into the database with the new scenario IDs
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
logger = logging.getLogger("rcp-ensemble-creator")

def get_unique_gcm_ssp_combinations():
    """Get all unique combinations of GCM and SSP in the database."""
    query = """
    SELECT DISTINCT gcm, ssp
    FROM scenarios
    WHERE gcm NOT LIKE 'ensemble%' AND ssp != 'ensemble'  -- Exclude any existing ensemble scenarios
    ORDER BY gcm, ssp
    """
    return DBManager.query_df(query)

def check_if_ensemble_exists(ensemble_name):
    """Check if an ensemble scenario with the given name already exists."""
    query = "SELECT scenario_id FROM scenarios WHERE scenario_name = ?"
    result = DBManager.query_df(query, [ensemble_name])
    
    if not result.empty:
        logger.info(f"Ensemble scenario '{ensemble_name}' already exists with ID: {result['scenario_id'].iloc[0]}")
        return int(result['scenario_id'].iloc[0])
    
    return None

def get_scenarios_for_gcm_ssp(gcm, ssp):
    """Get all scenarios for a given GCM and SSP combination."""
    query = """
    SELECT scenario_id, scenario_name, rcp
    FROM scenarios
    WHERE gcm = ? AND ssp = ?
    """
    return DBManager.query_df(query, [gcm, ssp])

def create_ensemble_scenario(ensemble_name, gcm, ssp):
    """Create a new scenario record for an RCP ensemble scenario."""
    # Get the next available scenario ID
    query = "SELECT MAX(scenario_id) + 1 AS next_id FROM scenarios"
    result = DBManager.query_df(query)
    ensemble_id = int(result['next_id'].iloc[0])
    
    # Insert the new scenario
    insert_query = """
    INSERT INTO scenarios (scenario_id, scenario_name, gcm, rcp, ssp, description)
    VALUES (?, ?, ?, 'ensemble_rcp', ?, ?)
    """
    
    description = f"Mean transition across all RCPs for {gcm} and {ssp}"
    
    with DBManager.connection() as conn:
        conn.execute(insert_query, [ensemble_id, ensemble_name, gcm, ssp, description])
    
    logger.info(f"Created new ensemble scenario '{ensemble_name}' with ID: {ensemble_id}")
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
    next_transition_id = int(max_id_result['next_id'].iloc[0])
    
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

def handle_ensemble_for_gcm_ssp(gcm, ssp, force_recreate=False):
    """Handle the creation of an ensemble scenario for a specific GCM and SSP combination."""
    ensemble_name = f"ensemble_rcp_{gcm}_{ssp}"
    
    # Check if this ensemble already exists
    existing_id = check_if_ensemble_exists(ensemble_name)
    if existing_id is not None and not force_recreate:
        user_input = input(f"Ensemble scenario '{ensemble_name}' already exists with ID {existing_id}. Recreate? (y/n): ")
        if user_input.lower() != 'y':
            logger.info(f"Skipping creation of ensemble '{ensemble_name}'")
            return
    
    # Delete existing ensemble if needed
    if existing_id is not None:
        logger.info(f"Deleting existing ensemble scenario '{ensemble_name}'")
        with DBManager.connection() as conn:
            conn.execute("DELETE FROM land_use_transitions WHERE scenario_id = ?", [existing_id])
            conn.execute("DELETE FROM scenarios WHERE scenario_id = ?", [existing_id])
    
    # Get scenarios for this GCM+SSP combination
    scenarios_df = get_scenarios_for_gcm_ssp(gcm, ssp)
    scenario_ids = scenarios_df['scenario_id'].tolist()
    
    if not scenario_ids:
        logger.warning(f"No scenarios found for GCM={gcm}, SSP={ssp}")
        return
    
    logger.info(f"Found {len(scenario_ids)} scenarios for GCM={gcm}, SSP={ssp}: {', '.join(scenarios_df['rcp'].tolist())}")
    
    # Create the new ensemble scenario
    ensemble_id = create_ensemble_scenario(ensemble_name, gcm, ssp)
    
    # Calculate and insert the ensemble transitions
    calculate_and_insert_ensemble_transitions(ensemble_id, scenario_ids)
    
    logger.info(f"RCP ensemble scenario '{ensemble_name}' created successfully with ID: {ensemble_id}")
    return ensemble_id

def main():
    """Main function to create RCP ensemble scenarios."""
    logger.info("Starting creation of RCP ensemble scenarios")
    
    # Get all unique GCM+SSP combinations
    combinations = get_unique_gcm_ssp_combinations()
    logger.info(f"Found {len(combinations)} unique GCM+SSP combinations")
    
    # For each combination, create an ensemble
    for _, row in combinations.iterrows():
        gcm = row['gcm']
        ssp = row['ssp']
        logger.info(f"Processing GCM={gcm}, SSP={ssp}")
        handle_ensemble_for_gcm_ssp(gcm, ssp)
    
    # Optimize the database after all inserts
    logger.info("Optimizing database")
    SchemaManager.optimize_database()
    
    logger.info("All RCP ensemble scenarios created successfully")

if __name__ == "__main__":
    main() 