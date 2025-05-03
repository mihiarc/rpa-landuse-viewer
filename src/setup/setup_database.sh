#!/bin/bash
# Setup script for RPA Land Use Viewer database

set -e  # Exit on error

# Initialize Python virtual environment using uv
echo "Creating virtual environment..."
if [ ! -d ".venv" ]; then
    uv venv
    # If uv is not installed, use this as fallback
    if [ $? -ne 0 ]; then
        echo "uv not found, attempting to install it..."
        pip install uv
        uv venv
    fi
fi

# Activate the virtual environment
source .venv/bin/activate

# Install requirements
echo "Installing dependencies..."
# Install the package in development mode
uv pip install -e .
# Install additional dependencies if needed
uv pip install duckdb pandas tqdm python-dotenv pyarrow

# Create database directory if not existing
mkdir -p data/database

# Check for init.sql
if [ ! -f "data/database/init.sql" ]; then
    echo "Error: data/database/init.sql not found"
    exit 1
fi

# Initialize the database
echo "Initializing database..."
python -m src.db.initialize_database --optimize

# Import the data if the file exists
PARQUET_FILE="data/raw/rpa_landuse_data_filtered.parquet"
if [ -f "$PARQUET_FILE" ]; then
    echo "Importing filtered Parquet data (this may take a while)..."
    # Use the filtered Parquet data file for import
    python -m src.db.import_parquet_data --input "$PARQUET_FILE"
else
    echo "Warning: Filtered Parquet data file not found at $PARQUET_FILE"
    echo "Database initialized without data. Run import manually when data is available."
fi

# Clean up database (remove redundant columns)
echo "Cleaning up database..."
python -c "
import duckdb

# Connect to database once for all operations
conn = duckdb.connect('data/database/rpa.db')

# Remove redundant t1 and t2 columns
print('\nChecking for redundant columns...')
col_check = conn.execute(\"\"\"
    SELECT COUNT(*) 
    FROM pragma_table_info('landuse_change') 
    WHERE name IN ('t1', 't2')
\"\"\").fetchone()[0]

if col_check > 0:
    print('Removing redundant t1 and t2 columns (totals can be calculated from transition data)...')
    try:
        conn.execute('ALTER TABLE landuse_change DROP COLUMN t1')
        conn.execute('ALTER TABLE landuse_change DROP COLUMN t2')
        print('Columns removed successfully.')
    except Exception as e:
        print(f'Error removing columns: {e}')
else:
    print('Columns t1 and t2 not found in the landuse_change table.')

# Remove t1 and t2 from landuse_types table
print('\nRemoving t1 and t2 from landuse_types table...')
try:
    conn.execute(\"DELETE FROM landuse_types WHERE landuse_type_code IN ('t1', 't2')\")
    print('Removed t1 and t2 from landuse_types successfully.')
except Exception as e:
    print(f'Error removing t1/t2 from categories: {e}')

# Close connection
conn.close()
print('\nDatabase cleanup completed successfully.')
"

echo "Database setup complete!" 