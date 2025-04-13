#!/usr/bin/env python3
"""
RPA Land Use Data Setup Script

This script orchestrates the one-time setup process for the RPA Land Use data:
1. Converts JSON data to Parquet format
2. Validates the converted data
3. Loads the data into MySQL database
4. Verifies the database loading
"""

import os
import sys
import argparse
import logging
import time
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('data_setup.log')
    ]
)
logger = logging.getLogger(__name__)

# Add the parent directory to sys.path to import from parent package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from data_setup.converter import convert_json_to_parquet
    from data_setup.validator import validate_parquet_data
    from data_setup.db_loader import load_to_mysql, verify_database_load
except ImportError as e:
    logger.error(f"Error importing modules: {e}")
    sys.exit(1)

def setup_data(json_file, parquet_file, skip_validation=False, skip_db_load=False):
    """
    Run the full data setup process.
    
    Args:
        json_file (str): Path to input JSON file
        parquet_file (str): Path to output Parquet file
        skip_validation (bool): Skip data validation step
        skip_db_load (bool): Skip database loading step
        
    Returns:
        dict: Setup results
    """
    start_time = time.time()
    results = {
        "success": True,
        "steps_completed": [],
        "steps_skipped": [],
        "errors": []
    }
    
    # Create directories if they don't exist
    Path(parquet_file).parent.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Convert JSON to Parquet
    logger.info("STEP 1: Converting JSON to Parquet")
    try:
        conversion_stats = convert_json_to_parquet(json_file, parquet_file)
        results["steps_completed"].append("conversion")
        results["conversion_stats"] = conversion_stats
        logger.info("Conversion completed successfully")
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        results["success"] = False
        results["errors"].append(f"Conversion error: {str(e)}")
        return results
    
    # Step 2: Validate Parquet data
    if skip_validation:
        logger.info("STEP 2: Skipping validation (as requested)")
        results["steps_skipped"].append("validation")
    else:
        logger.info("STEP 2: Validating Parquet data")
        try:
            validation_results = validate_parquet_data(parquet_file)
            results["steps_completed"].append("validation")
            results["validation_results"] = validation_results
            
            if not validation_results["valid"]:
                logger.warning("Validation found issues with the data")
                logger.warning("Consider reviewing these issues before proceeding")
                for issue in validation_results["issues"]:
                    logger.warning(f"- {issue}")
                
                # Ask for confirmation
                if not skip_db_load:
                    confirm = input("Data validation found issues. Continue with database loading? (y/n): ")
                    if confirm.lower() != 'y':
                        logger.info("Database loading skipped by user")
                        results["steps_skipped"].append("db_loading")
                        skip_db_load = True
            else:
                logger.info("Validation completed successfully: No issues found")
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            results["errors"].append(f"Validation error: {str(e)}")
            # Still continue to DB load
    
    # Step 3: Load data to MySQL
    if skip_db_load:
        logger.info("STEP 3: Skipping database loading (as requested)")
        results["steps_skipped"].append("db_loading")
    else:
        logger.info("STEP 3: Loading data to MySQL")
        try:
            db_stats = load_to_mysql(parquet_file)
            results["steps_completed"].append("db_loading")
            results["db_stats"] = db_stats
            
            if db_stats["success"]:
                logger.info("Database loading completed successfully")
                
                # Step 4: Verify database load
                logger.info("STEP 4: Verifying database load")
                verify_results = verify_database_load()
                results["steps_completed"].append("verification")
                results["verification_results"] = verify_results
                
                if verify_results["success"]:
                    logger.info("Database verification completed successfully")
                else:
                    logger.error("Database verification failed")
                    results["errors"].append(f"Verification error: {verify_results.get('error', 'Unknown error')}")
            else:
                logger.error("Database loading failed")
                results["errors"].append(f"Database loading error: {db_stats.get('error', 'Unknown error')}")
                results["success"] = False
        except Exception as e:
            logger.error(f"Database loading failed: {e}")
            results["errors"].append(f"Database loading error: {str(e)}")
            results["success"] = False
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    results["elapsed_time"] = elapsed_time
    
    return results

def main():
    """Main function to run the setup script."""
    parser = argparse.ArgumentParser(description='RPA Land Use Data Setup Script')
    parser.add_argument('--json', default='data/raw/county_landuse_projections_RPA.json',
                      help='Input JSON file path')
    parser.add_argument('--parquet', default='data/processed/rpa_landuse_data.parquet',
                      help='Output Parquet file path')
    parser.add_argument('--skip-validation', action='store_true',
                      help='Skip data validation step')
    parser.add_argument('--skip-db-load', action='store_true',
                      help='Skip database loading step')
    args = parser.parse_args()
    
    logger.info("Starting RPA Land Use Data Setup")
    results = setup_data(args.json, args.parquet, args.skip_validation, args.skip_db_load)
    
    # Print summary
    logger.info("\n" + "="*50)
    logger.info("SETUP PROCESS SUMMARY")
    logger.info("="*50)
    
    logger.info(f"Overall Status: {'Success' if results['success'] else 'Failed'}")
    logger.info(f"Elapsed Time: {results['elapsed_time']:.2f} seconds")
    
    logger.info("\nSteps Completed:")
    for step in results["steps_completed"]:
        logger.info(f"- {step}")
    
    if results["steps_skipped"]:
        logger.info("\nSteps Skipped:")
        for step in results["steps_skipped"]:
            logger.info(f"- {step}")
    
    if results["errors"]:
        logger.info("\nErrors:")
        for error in results["errors"]:
            logger.info(f"- {error}")
    
    if "db_stats" in results and results["db_stats"]["success"]:
        logger.info("\nDatabase Loading Statistics:")
        db_stats = results["db_stats"]
        logger.info(f"Records processed: {db_stats['record_count']:,}")
        logger.info(f"Scenarios inserted: {db_stats['scenarios_inserted']}")
        logger.info(f"Time steps inserted: {db_stats['time_steps_inserted']}")
        logger.info(f"Counties inserted: {db_stats['counties_inserted']}")
        logger.info(f"Transitions inserted: {db_stats['transitions_inserted']:,}")
    
    logger.info("="*50)
    
    if results["success"]:
        logger.info("Setup completed successfully!")
    else:
        logger.error("Setup failed. Please check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main() 