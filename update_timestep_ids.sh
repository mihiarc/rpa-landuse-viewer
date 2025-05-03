#!/bin/bash
# Script to reindex time step IDs in the RPA Land Use Viewer database
# This ensures time_step_id values are sequential after removing calibration periods

set -e  # Exit on error

# Activate the virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
    echo "Using virtual environment: .venv"
else
    echo "Warning: No virtual environment found at .venv"
fi

# Check if database exists
if [ ! -f "data/database/rpa.db" ]; then
    echo "Error: Database not found at data/database/rpa.db"
    exit 1
fi

echo "Reindexing time step IDs..."

# Create a backup of the database first
DB_PATH="data/database/rpa.db"
BACKUP_PATH="data/database/rpa_backup_$(date +%Y%m%d_%H%M%S).db"
cp "$DB_PATH" "$BACKUP_PATH"
echo "Database backup created at $BACKUP_PATH"

# Run the reindexing operation with a simpler approach
python -c "
import duckdb
import sys
import os
import shutil

try:
    # Connect to original database
    conn = duckdb.connect('$DB_PATH')
    print('Connected to database.')
    
    # Get the current time steps and create mapping
    print('\\nCurrent time steps:')
    time_steps = conn.execute('''
        SELECT time_step_id, start_year, end_year, time_step_name 
        FROM time_steps
        ORDER BY start_year
    ''').fetchall()
    
    # Create a mapping from old to new IDs
    id_mapping = {}
    new_time_steps = []
    
    for i, ts in enumerate(time_steps, 1):
        old_id = ts[0]
        id_mapping[old_id] = i
        new_time_steps.append((i, ts[1], ts[2], ts[3]))
        print(f'Mapping: Old ID {old_id} -> New ID {i}, Year: {ts[1]}, Name: {ts[3]}')
    
    # Check if any IDs need to be updated
    needs_update = False
    for old_id, new_id in id_mapping.items():
        if old_id != new_id:
            needs_update = True
            break
    
    if not needs_update:
        print('\\nNo ID updates needed - current IDs are already sequential.')
        sys.exit(0)
    
    # Create a temporary database with the updated schema
    temp_db_path = 'data/database/rpa_temp.db'
    if os.path.exists(temp_db_path):
        os.remove(temp_db_path)
    
    # Connect to temp database
    temp_conn = duckdb.connect(temp_db_path)
    
    # Export the schema from original database and create tables
    print('\\nCreating schema in temporary database...')

    # Manually recreate the schema in the new database
    temp_conn.execute('''
        CREATE TABLE counties (
            fips_code VARCHAR PRIMARY KEY NOT NULL,
            county_name VARCHAR,
            state_name VARCHAR,
            state_fips VARCHAR,
            region VARCHAR
        )
    ''')

    temp_conn.execute('''
        CREATE TABLE scenarios (
            scenario_id INTEGER PRIMARY KEY NOT NULL,
            scenario_name VARCHAR NOT NULL,
            gcm VARCHAR NOT NULL,
            rcp VARCHAR NOT NULL,
            ssp VARCHAR NOT NULL,
            description VARCHAR
        )
    ''')

    temp_conn.execute('''
        CREATE TABLE time_steps (
            time_step_id INTEGER PRIMARY KEY NOT NULL,
            time_step_name VARCHAR NOT NULL,
            start_year INTEGER NOT NULL,
            end_year INTEGER NOT NULL
        )
    ''')

    temp_conn.execute('''
        CREATE TABLE land_use_categories (
            category_code VARCHAR PRIMARY KEY NOT NULL,
            category_name VARCHAR NOT NULL,
            description VARCHAR
        )
    ''')

    temp_conn.execute('''
        CREATE TABLE land_use_transitions (
            transition_id INTEGER PRIMARY KEY NOT NULL,
            scenario_id INTEGER NOT NULL,
            time_step_id INTEGER NOT NULL,
            fips_code VARCHAR NOT NULL,
            from_land_use VARCHAR NOT NULL,
            to_land_use VARCHAR NOT NULL,
            area_hundreds_acres DOUBLE NOT NULL,
            FOREIGN KEY (scenario_id) REFERENCES scenarios(scenario_id),
            FOREIGN KEY (fips_code) REFERENCES counties(fips_code),
            FOREIGN KEY (time_step_id) REFERENCES time_steps(time_step_id),
            FOREIGN KEY (from_land_use) REFERENCES land_use_categories(category_code),
            FOREIGN KEY (to_land_use) REFERENCES land_use_categories(category_code)
        )
    ''')
    
    # Copy data for all tables except land_use_transitions and time_steps
    tables = conn.execute('''
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        AND name NOT IN ('land_use_transitions', 'time_steps')
    ''').fetchall()
    
    for table in tables:
        table_name = table[0]
        print(f'Copying data for table: {table_name}')
        data = conn.execute(f'SELECT * FROM {table_name}').fetchall()
        if data:
            columns = conn.execute(f'PRAGMA table_info({table_name})').fetchall()
            column_names = [col[1] for col in columns]
            column_str = ', '.join(column_names)
            placeholders = ', '.join(['?' for _ in column_names])
            
            for row in data:
                temp_conn.execute(f'INSERT INTO {table_name} ({column_str}) VALUES ({placeholders})', row)
    
    # Insert time_steps with new IDs
    print('Inserting time_steps with updated IDs...')
    for ts in new_time_steps:
        temp_conn.execute('''
            INSERT INTO time_steps (time_step_id, start_year, end_year, time_step_name)
            VALUES (?, ?, ?, ?)
        ''', ts)
    
    # Copy land_use_transitions with updated time_step_id references
    print('Copying land_use_transitions with updated time_step_id references...')
    transitions = conn.execute('''
        SELECT transition_id, scenario_id, time_step_id, fips_code, 
               from_land_use, to_land_use, area_hundreds_acres 
        FROM land_use_transitions
    ''').fetchall()
    
    for transition in transitions:
        transition_id, scenario_id, old_time_step_id, fips_code, from_lu, to_lu, area = transition
        new_time_step_id = id_mapping.get(old_time_step_id, old_time_step_id)
        
        temp_conn.execute('''
            INSERT INTO land_use_transitions 
            (transition_id, scenario_id, time_step_id, fips_code, from_land_use, to_land_use, area_hundreds_acres)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (transition_id, scenario_id, new_time_step_id, fips_code, from_lu, to_lu, area))
    
    # Close connections
    conn.close()
    temp_conn.close()
    
    # Replace original database with temp database
    print('\\nReplacing original database with updated database...')
    os.remove('$DB_PATH')
    shutil.copy(temp_db_path, '$DB_PATH')
    os.remove(temp_db_path)
    
    print('\\nTime step IDs reindexed successfully!')
    print(f'Original database backed up to {os.path.basename(\"$BACKUP_PATH\")}')
    
except Exception as e:
    print(f'\\nError: {e}')
    sys.exit(1)
"

echo "Script completed." 