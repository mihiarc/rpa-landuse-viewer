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
uv pip install duckdb pandas tqdm python-dotenv

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
if [ -f "data/raw/county_landuse_projections_RPA.json" ]; then
    echo "Importing land use data (this may take a while)..."
    python -m src.db.import_landuse_data
else
    echo "Warning: Raw data file not found at data/raw/county_landuse_projections_RPA.json"
    echo "Database initialized without data. Run import manually when data is available."
fi

# Clean up database (remove calibration period and redundant columns)
echo "Cleaning up database..."
python -c "
import duckdb

# Connect to database once for all operations
conn = duckdb.connect('data/database/rpa.db')

# 1. Remove the calibration period (2012)
print('Identifying time_step_id for 2012...')
time_step_id = conn.execute('SELECT time_step_id FROM time_steps WHERE start_year = 2012').fetchone()[0]
print(f'Deleting data for time_step_id={time_step_id}...')
conn.execute('DELETE FROM land_use_transitions WHERE time_step_id = ?', [time_step_id])
conn.execute('DELETE FROM time_steps WHERE start_year = 2012')
print('Calibration period removed successfully.')

# Reindex the time step IDs to maintain sequential integrity
print('\nReindexing time step IDs...')
conn.execute('''
    CREATE TEMPORARY TABLE temp_time_steps AS 
    SELECT ROW_NUMBER() OVER (ORDER BY start_year, end_year) AS new_id, 
           time_step_id AS old_id, start_year, end_year, notes 
    FROM time_steps
''')

# Update references in land_use_transitions
conn.execute('''
    UPDATE land_use_transitions
    SET time_step_id = (
        SELECT new_id 
        FROM temp_time_steps 
        WHERE old_id = land_use_transitions.time_step_id
    )
''')

# Update the time_steps table
conn.execute('''
    DELETE FROM time_steps
''')

conn.execute('''
    INSERT INTO time_steps (time_step_id, start_year, end_year, notes)
    SELECT new_id, start_year, end_year, notes
    FROM temp_time_steps
''')

# Drop temporary table
conn.execute('DROP TABLE temp_time_steps')
print('Time step IDs reindexed successfully.')

# 2. Remove redundant t1 and t2 columns
print('\nChecking for redundant columns...')
col_check = conn.execute(\"\"\"
    SELECT COUNT(*) 
    FROM pragma_table_info('land_use_transitions') 
    WHERE name IN ('t1', 't2')
\"\"\").fetchone()[0]

if col_check > 0:
    print('Removing redundant t1 and t2 columns (totals can be calculated from transition data)...')
    try:
        conn.execute('ALTER TABLE land_use_transitions DROP COLUMN t1')
        conn.execute('ALTER TABLE land_use_transitions DROP COLUMN t2')
        print('Columns removed successfully.')
    except Exception as e:
        print(f'Error removing columns: {e}')
else:
    print('Columns t1 and t2 not found in the land_use_transitions table.')

# 3. Remove t1 and t2 from land_use_categories table
print('\nRemoving t1 and t2 from land_use_categories table...')
try:
    conn.execute("DELETE FROM land_use_categories WHERE category_code IN ('t1', 't2')")
    print('Removed t1 and t2 from land_use_categories successfully.')
except Exception as e:
    print(f'Error removing t1/t2 from categories: {e}')

# Close connection
conn.close()
print('\nDatabase cleanup completed successfully.')
"

echo "Database setup complete!" 