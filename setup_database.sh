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

# 1a. Create new sequential time step IDs
print('\nReindexing time step IDs...')
# Get current time steps ordered by start_year
time_steps = conn.execute('''
    SELECT time_step_id, start_year, end_year, time_step_name 
    FROM time_steps 
    ORDER BY start_year
''').fetchall()

# Create mapping from old to new IDs
id_mapping = {}
for i, ts in enumerate(time_steps, 1):
    old_id = ts[0]
    id_mapping[old_id] = i
    print(f'Mapping: Old ID {old_id} -> New ID {i}, Year: {ts[1]}, Name: {ts[3]}')

# Only proceed if IDs need to be changed
needs_update = False
for old_id, new_id in id_mapping.items():
    if old_id != new_id:
        needs_update = True
        break

if needs_update:
    print('Updating time step IDs...')
    
    # Create temporary database and copy all data with new time step IDs
    temp_db_path = 'data/database/rpa_temp.db'
    conn.execute(f\"EXPORT DATABASE '{temp_db_path}'\")
    temp_conn = duckdb.connect(temp_db_path)
    
    # Update time_step_id values in the temporary database
    for old_id, new_id in id_mapping.items():
        if old_id != new_id:
            # Update time_steps table
            temp_conn.execute('''
                UPDATE time_steps 
                SET time_step_id = ? 
                WHERE time_step_id = ?
            ''', [new_id, old_id])
            
            # Update references in land_use_transitions
            temp_conn.execute('''
                UPDATE land_use_transitions 
                SET time_step_id = ? 
                WHERE time_step_id = ?
            ''', [new_id, old_id])
    
    # Close connections
    conn.close()
    temp_conn.close()
    
    # Replace original database with temp database
    import os
    import shutil
    os.remove('data/database/rpa.db')
    shutil.copy(temp_db_path, 'data/database/rpa.db')
    os.remove(temp_db_path)
    
    # Reconnect to the updated database
    conn = duckdb.connect('data/database/rpa.db')
    print('Time step IDs reindexed successfully.')
else:
    print('No reindexing needed - time step IDs are already sequential.')

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
    conn.execute(\"DELETE FROM land_use_categories WHERE category_code IN ('t1', 't2')\")
    print('Removed t1 and t2 from land_use_categories successfully.')
except Exception as e:
    print(f'Error removing t1/t2 from categories: {e}')

# Close connection
conn.close()
print('\nDatabase cleanup completed successfully.')
"

echo "Database setup complete!" 