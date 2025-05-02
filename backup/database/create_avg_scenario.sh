#!/bin/bash

# Create average scenario view script
# This script adds an average scenario to the database by calculating the average land use transition
# across all scenarios to provide an "ensemble average" view.

# Get the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DB_PATH="${SCRIPT_DIR}/rpa_landuse.db"
SQL_SCRIPT="${SCRIPT_DIR}/create_avg_scenario_view.sql"

# Check if database exists
if [ ! -f "$DB_PATH" ]; then
    echo "Error: Database file not found at $DB_PATH"
    exit 1
fi

# Check if SQL script exists
if [ ! -f "$SQL_SCRIPT" ]; then
    echo "Error: SQL script not found at $SQL_SCRIPT"
    exit 1
fi

echo "Creating average scenario in database..."
duckdb "$DB_PATH" < "$SQL_SCRIPT"

echo "Done!" 