# DuckDB CLI Cheatsheet

## Getting Started

```bash
# Start the CLI with a database
duckdb data/database/rpa_landuse_duck.db

# Exit the CLI
.exit or .quit
```

## Basic Commands

| Command | Description |
|---------|-------------|
| `.help` | Display help information |
| `.tables` | List all tables in the database |
| `.schema TABLE` | Show table structure |
| `.mode csv/column/line` | Change output format |
| `.headers on/off` | Toggle column headers |
| `.timer on/off` | Toggle query timing |
| `.output FILE` | Send output to file |
| `.output stdout` | Reset output to console |
| `.read FILE.sql` | Run SQL from file |

## Essential SQL

```sql
-- Basic SELECT
SELECT * FROM table_name LIMIT 10;

-- Filter with WHERE
SELECT * FROM counties WHERE state_name = 'California';

-- JOIN tables
SELECT 
    c.county_name, 
    t.from_land_use, 
    t.to_land_use 
FROM 
    land_use_transitions t
JOIN 
    counties c ON t.fips_code = c.fips_code
LIMIT 10;

-- Aggregations
SELECT 
    state_name, 
    COUNT(*) as county_count 
FROM 
    counties 
GROUP BY 
    state_name
ORDER BY 
    county_count DESC;
```

## Data Modification

```sql
-- Delete rows based on a condition
DELETE FROM table_name WHERE condition;

-- Example: Delete all rows with state_name 'Alaska'
DELETE FROM counties WHERE state_name = 'Alaska';

-- Delete rows with multiple conditions
DELETE FROM land_use_transitions 
WHERE scenario_id = 5 
AND area_hundreds_acres < 1.0;

-- Delete all rows in a table (keeps the table structure)
DELETE FROM table_name;

-- Drop (delete) an entire table
DROP TABLE table_name;

-- Drop a table only if it exists
DROP TABLE IF EXISTS table_name;
```

## Exporting Data

```sql
-- Export to CSV
.mode csv
.headers on
.output results.csv
SELECT * FROM counties LIMIT 100;
.output stdout

-- Export to Parquet
COPY (SELECT * FROM counties) TO 'counties.parquet' (FORMAT PARQUET);
```

## Land Use Database Quick Queries

```sql
-- All scenarios
SELECT * FROM scenarios;

-- Time periods
SELECT * FROM time_steps;

-- Land use categories
SELECT * FROM land_use_categories;

-- Top transitions for scenario 1 and time step 2
SELECT 
    c.county_name, 
    c.state_name, 
    t.from_land_use, 
    t.to_land_use, 
    t.area_hundreds_acres * 100 as acres
FROM 
    land_use_transitions t
JOIN 
    counties c ON t.fips_code = c.fips_code
WHERE 
    t.scenario_id = 1 
    AND t.time_step_id = 2
    AND t.from_land_use != t.to_land_use
ORDER BY 
    acres DESC
LIMIT 10;
``` 