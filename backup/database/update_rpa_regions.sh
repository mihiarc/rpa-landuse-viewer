#!/bin/bash
# Script to add RPA regions to the database

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

# Run the SQL script to add RPA regions
echo "Adding RPA regions to database..."
duckdb "$DB_PATH" < "$SCRIPT_DIR/add_rpa_regions.sql"

# Verify the views were created
echo "Verifying created views..."
duckdb "$DB_PATH" <<EOF
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'main' AND table_name LIKE 'rpa%';
EOF

echo "RPA regions added successfully!" 