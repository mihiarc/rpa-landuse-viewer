#!/usr/bin/env python3
"""
Database optimization script for RPA Land Use Viewer.

This script optimizes the database by:
1. Ensuring all required indexes exist
2. Running ANALYZE to update statistics
3. Running VACUUM to reclaim space and optimize storage
4. Setting optimal pragmas for performance
"""

import os
import sys
import logging
import argparse
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.db.schema_manager import SchemaManager
from src.db.database import DBManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("database-optimizer")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Optimize the RPA Land Use database")
    parser.add_argument("--vacuum", action="store_true", help="Run VACUUM (can take a long time)")
    parser.add_argument("--indexes", action="store_true", help="Create or update indexes")
    parser.add_argument("--analyze", action="store_true", help="Run ANALYZE to update statistics")
    parser.add_argument("--all", action="store_true", help="Run all optimizations")
    return parser.parse_args()

def main():
    """Main function."""
    args = parse_args()
    
    # If no arguments provided, show help
    if not (args.vacuum or args.indexes or args.analyze or args.all):
        logger.info("No optimization tasks specified. Use --all to run all optimizations.")
        return
    
    # If --all is specified, run everything
    if args.all:
        args.vacuum = True
        args.indexes = True
        args.analyze = True
    
    # Ensure database exists
    db_path = DBManager._ensure_db_exists()
    logger.info(f"Optimizing database at {db_path}")
    
    # Create or update indexes
    if args.indexes:
        logger.info("Creating or updating indexes")
        SchemaManager.ensure_indexes()
        logger.info("Indexes updated")
    
    # Run ANALYZE
    if args.analyze:
        logger.info("Analyzing database for improved query planning")
        with DBManager.connection() as conn:
            conn.execute("ANALYZE")
        logger.info("Database analyzed")
    
    # Run VACUUM
    if args.vacuum:
        logger.info("Vacuuming database (this may take a while)")
        with DBManager.connection() as conn:
            conn.execute("VACUUM")
        logger.info("Database vacuumed")
    
    logger.info("Database optimization complete")

if __name__ == "__main__":
    main() 