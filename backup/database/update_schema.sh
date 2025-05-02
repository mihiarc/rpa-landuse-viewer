#!/bin/bash

# Script to update the database schema with state grouping capabilities

# Exit on error
set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Define database path
# Check if DB_PATH environment variable is set
if [ -z "$DB_PATH" ]; then
    DB_PATH="$SCRIPT_DIR/rpa_landuse_duck.db"
fi

echo "Using database at: $DB_PATH"

# Add state groupings
echo "Adding state groupings to database..."
duckdb "$DB_PATH" < "$SCRIPT_DIR/add_state_groups.sql"

# Populate states table
echo "Populating states table..."
duckdb "$DB_PATH" < "$SCRIPT_DIR/populate_states.sql"

# Verify the views were created
echo "Verifying created views..."
duckdb "$DB_PATH" <<EOF
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'main' AND table_name LIKE '%state%';
EOF

# Verify the indexes were created
echo "Verifying indexes..."
duckdb "$DB_PATH" <<EOF
SELECT index_name 
FROM information_schema.indexes 
WHERE index_name = 'idx_counties_state_fips';
EOF

echo "Schema update completed successfully!" 