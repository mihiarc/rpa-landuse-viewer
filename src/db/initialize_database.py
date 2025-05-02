#!/usr/bin/env python3
"""
Database initialization script for RPA Land Use Viewer.

This script initializes a fresh database by:
1. Creating the core schema tables
2. Adding essential indexes
3. Running required setup scripts
"""

import os
import sys
import logging
import argparse
from pathlib import Path
from glob import glob

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
logger = logging.getLogger("database-initializer")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Initialize the RPA Land Use database")
    parser.add_argument("--with-migrations", action="store_true", 
                        help="Run all migration scripts after initialization")
    parser.add_argument("--optimize", action="store_true", 
                        help="Run optimization steps after initialization")
    return parser.parse_args()

def find_migration_scripts():
    """Find all SQL migration scripts in the database directory."""
    db_dir = Path("data/database")
    migration_scripts = []
    
    # Skip init.sql since it's handled separately
    for sql_file in db_dir.glob("*.sql"):
        if sql_file.name != "init.sql":
            migration_scripts.append(sql_file)
    
    # Sort them by name to ensure proper order
    migration_scripts.sort()
    
    return migration_scripts

def main():
    """Main function."""
    args = parse_args()
    
    # Initialize database schema
    logger.info("Initializing database schema")
    SchemaManager.initialize_database()
    logger.info("Database schema initialized")
    
    # Set up indexes
    logger.info("Creating database indexes")
    SchemaManager.ensure_indexes()
    logger.info("Database indexes created")
    
    # Run migrations if requested
    if args.with_migrations:
        logger.info("Running migration scripts")
        migration_scripts = find_migration_scripts()
        if migration_scripts:
            SchemaManager.run_migration_scripts([str(script) for script in migration_scripts])
            logger.info(f"Completed {len(migration_scripts)} migration scripts")
        else:
            logger.info("No migration scripts found")
    
    # Optimize if requested
    if args.optimize:
        logger.info("Optimizing database")
        SchemaManager.optimize_database()
        logger.info("Database optimization complete")
    
    logger.info("Database initialization complete")
    logger.info(f"Database location: {DBManager._ensure_db_exists()}")

if __name__ == "__main__":
    main() 