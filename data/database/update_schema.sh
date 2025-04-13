#!/bin/bash

# Script to update the database schema with state grouping capabilities

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
DB_PATH="$SCRIPT_DIR/rpa_landuse.db"

# Check if database exists
if [ ! -f "$DB_PATH" ]; then
    echo "Error: Database file not found at $DB_PATH"
    exit 1
fi

echo "Updating schema for state groupings..."

# Add state table and views
echo "Adding state table and views..."
sqlite3 "$DB_PATH" < "$SCRIPT_DIR/add_state_groups.sql"

# Populate states table with data
echo "Populating states table..."
sqlite3 "$DB_PATH" < "$SCRIPT_DIR/populate_states.sql"

# Verify the changes
echo "Verifying changes..."
sqlite3 "$DB_PATH" <<EOF
.mode column
.headers on
-- Check if states table was created and populated
SELECT COUNT(*) AS state_count FROM states;

-- Check if views were created
SELECT name FROM sqlite_master WHERE type='view' AND 
    name IN ('county_state_map', 'state_land_use_transitions', 'region_hierarchy', 'counties_by_state', 'state_land_use_summary');

-- Check if index was created
SELECT name FROM sqlite_master WHERE type='index' AND name='idx_counties_state_fips';
EOF

echo "Schema update complete." 