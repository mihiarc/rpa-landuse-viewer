# DuckDB Testing Suite

This directory contains a comprehensive testing suite for DuckDB databases used in the RPA Land Use Viewer application. The tests allow you to validate the database structure, test queries, and ensure data integrity independently from the Streamlit application.

## Overview

The testing suite consists of the following files:

- `test_duckdb.py`: A standalone test script that runs a series of tests against the DuckDB database
- `test_duckdb_unit.py`: Unit tests using pytest for DuckDB database connection, schema, and queries 
- `test_duckdb_schema.py`: Specific tests for validating database schema against expected definitions
- `run_tests.py`: A runner script to execute all or specific tests

## Prerequisites

- Python 3.11 or higher
- [UV](https://github.com/astral-sh/uv) for virtual environment and package management (recommended)
- DuckDB database file (defaults to `data/database/rpa_landuse_duck.db`)

## Setup

### Using UV (Recommended)

```bash
# Create a virtual environment
uv venv .venv-test --python 3.11

# Activate the virtual environment
# On Linux/macOS:
source .venv-test/bin/activate
# On Windows:
.venv-test\Scripts\activate

# Install the required packages
uv pip install pytest duckdb pandas python-dotenv
```

### Using the Helper Script

Alternatively, you can use the runner script to set up the environment:

```bash
python run_tests.py --setup-venv
```

## Configuration

The test suite uses environment variables for configuration:

- `DB_PATH`: Path to the DuckDB database file (default: `data/database/rpa_landuse_duck.db`)

You can set these in a `.env` file or pass them directly when running the tests.

## Running the Tests

### Running All Tests

To run all tests:

```bash
python run_tests.py --all
```

### Running Specific Tests

To run only the standalone test:

```bash
python run_tests.py --standalone
```

To run only the unit tests:

```bash
python run_tests.py --unit
```

To run only the schema validation tests:

```bash
python run_tests.py --schema
```

### Running Tests Directly

You can also run each test file directly:

```bash
# Run the standalone test
python test_duckdb.py

# Run pytest unit tests
pytest -xvs test_duckdb_unit.py

# Run pytest schema tests
pytest -xvs test_duckdb_schema.py
```

## Test Descriptions

### Standalone Test (`test_duckdb.py`)

This script performs a comprehensive check of the DuckDB database:

- Tests database connection
- Lists all tables in the database
- Validates table schemas
- Counts rows in each table
- Fetches sample data from each table
- Executes complex analytical queries
- Tests querying Pandas DataFrames with DuckDB

Example output:

```
================================================================================
DUCKDB DATABASE TEST RESULTS
================================================================================

Connection test: PASSED

Tables in the database (4):
  - scenarios
  - time_steps
  - counties
  - land_use_transitions

SCENARIOS Table:
  - Row count: 20
  - Sample data: 5 rows

TIME_STEPS Table:
  - Row count: 6
  - Sample data: 5 rows

COUNTIES Table:
  - Row count: 3108
  - Sample data: 5 rows

LAND_USE_TRANSITIONS Table:
  - Row count: 5436789
  - Sample data: 5 rows

Complex query test: PASSED
  - Returned 10 rows

Dataframe query test: PASSED
  - Returned 5 rows
  - Results:
    ('Rangeland', 'Urban', 300.0)
    ('Forest', 'Cropland', 250.3)
    ('Urban', 'Forest', 120.2)
    ('Cropland', 'Urban', 100.5)
    ('Pasture', 'Forest', 75.8)
```

### Unit Tests (`test_duckdb_unit.py`)

These tests focus on validating database functionality using pytest:

- Tests database connection
- Tests table creation
- Tests data insertion
- Tests basic queries
- Tests join queries
- Tests aggregation queries
- Tests querying Pandas DataFrames
- Tests transaction management (rollback and commit)

### Schema Validation Tests (`test_duckdb_schema.py`)

These tests specifically validate the database schema:

- Tests that all expected tables exist
- Tests that tables have the expected columns with correct types
- Tests that tables have data
- Tests that foreign key relationships function properly
- Tests that primary keys enforce uniqueness

## Example Database Schema

The tests expect the following database tables:

1. `scenarios`:
   - `scenario_id` (PRIMARY KEY)
   - `scenario_name`
   - `gcm` (Global Climate Model)
   - `rcp` (Representative Concentration Pathway)
   - `ssp` (Shared Socioeconomic Pathway)

2. `time_steps`:
   - `time_step_id` (PRIMARY KEY)
   - `start_year`
   - `end_year`

3. `counties`:
   - `fips_code` (PRIMARY KEY)
   - `county_name`

4. `land_use_transitions`:
   - `transition_id` (PRIMARY KEY)
   - `scenario_id` (FOREIGN KEY -> scenarios.scenario_id)
   - `time_step_id` (FOREIGN KEY -> time_steps.time_step_id)
   - `fips_code` (FOREIGN KEY -> counties.fips_code)
   - `from_land_use`
   - `to_land_use`
   - `acres`

## Extending the Tests

To add more tests:

1. For the standalone test, add new methods to the `DuckDBTester` class in `test_duckdb.py`
2. For unit tests, add new test functions to `test_duckdb_unit.py`
3. For schema tests, update the `EXPECTED_SCHEMAS` dictionary in `test_duckdb_schema.py` or add new test functions

## Troubleshooting

- If you encounter connection issues, check that the database file exists at the expected location
- For schema validation failures, verify that the database schema matches the expected schema
- For query errors, check that the table and column names match the actual database structure 