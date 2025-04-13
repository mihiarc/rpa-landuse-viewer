# SQLite Database Guide

This guide explains how to manage the SQLite database for the RPA Land Use Change Application.

## Database Overview

The application uses SQLite as its database engine. SQLite is a file-based, serverless database that provides a lightweight solution for data storage and retrieval.

- **Database File:** `data/database/rpa_landuse.db`
- **Test Database:** `data/database/test_rpa_landuse.db`

## SQLite Advantages

- No server setup or configuration required
- Self-contained in a single file
- Zero-configuration
- Cross-platform compatibility
- Built into Python (no additional dependencies)

## Working with the Database

### Initializing the Database

The database is automatically created and initialized when you run the data loading script:

```bash
# Run the database loader
python -m src.data_setup.db_loader
```

This script:
1. Creates the SQLite database file if it doesn't exist
2. Creates all necessary tables
3. Loads data from the Parquet file into the database

### Accessing SQLite Database

You can use the SQLite command-line tool to interact with the database directly:

```bash
# Connect to the database
sqlite3 data/database/rpa_landuse.db

# Some useful SQLite commands
.tables             # List all tables
.schema tablename   # Show schema for a table
.mode column        # Format output as columns
.headers on         # Show column headers
.quit               # Exit SQLite

# Run a query
SELECT COUNT(*) FROM land_use_transitions;
```

### Database Maintenance

```bash
# Create a backup (simply copy the file)
cp data/database/rpa_landuse.db data/database/backup_$(date +%Y%m%d).db

# Optimize the database
sqlite3 data/database/rpa_landuse.db "VACUUM;"

# Check for database integrity
sqlite3 data/database/rpa_landuse.db "PRAGMA integrity_check;"
```

## Database Schema

The database includes the following tables:

1. `scenarios`
   - Stores scenario information (GCM, RCP, SSP)
   - Primary key: scenario_id
   - Fields: scenario_name, gcm, rcp, ssp

2. `time_steps`
   - Tracks time periods
   - Primary key: time_step_id
   - Fields: start_year, end_year
   - Unique constraint on (start_year, end_year)

3. `counties`
   - Stores FIPS codes and county names
   - Primary key: fips_code
   - Fields: county_name

4. `land_use_transitions`
   - Main table storing land use changes
   - Primary key: transition_id
   - Foreign keys to scenarios, time_steps, and counties
   - Fields: scenario_id, time_step_id, fips_code, from_land_use, to_land_use, acres
   - Indexed for efficient querying

## Programmatic Access

Access the database from Python code:

```python
import sqlite3

# Connect to the database
conn = sqlite3.connect('data/database/rpa_landuse.db')
conn.row_factory = sqlite3.Row  # Return rows as dictionaries
cursor = conn.cursor()

# Run a query
cursor.execute("SELECT * FROM scenarios")
scenarios = cursor.fetchall()

# Work with the results
for scenario in scenarios:
    print(f"Scenario: {scenario['scenario_name']}")

# Close the connection
conn.close()
```

Or using the application's DatabaseConnection class:

```python
from src.db.database import DatabaseConnection

# Get a connection
conn = DatabaseConnection.get_connection()
cursor = conn.cursor()

# Run a query
cursor.execute("SELECT * FROM scenarios")
scenarios = cursor.fetchall()

# Close connection when done with the application
DatabaseConnection.close_connection()
```

## Troubleshooting

1. **Database file not found:**
   - Ensure you have run the data loader script
   - Check file permissions

2. **Cannot write to database:**
   - Verify you have write permissions to the database file and directory
   - Check disk space

3. **Performance issues:**
   - Run `VACUUM` to optimize the database
   - Ensure indexes are in place
   - Consider query optimization

4. **Database corruption:**
   - Restore from a backup
   - Run integrity check: `PRAGMA integrity_check;` 