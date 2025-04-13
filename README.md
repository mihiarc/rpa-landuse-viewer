# RPA Land Use Change Data Processing

This repository contains tools for processing and analyzing the USDA Forest Service's Resources Planning Act (RPA) land use change projection data. The dataset provides county-level land use transition projections for the conterminous United States from 2020 to 2070.

## Dataset Overview

The data represents gross land-use changes projected at the county level, based on an empirical econometric model of observed land-use transitions from 2001-2012 using National Resources Inventory (NRI) data. The projections include:

### Scenarios
The dataset includes 20 unique scenarios combining:
- Climate Models (GCM):
  - CNRM_CM5 ("wet" climate model)
  - HadGEM2_ES365 ("hot" climate model)
  - IPSL_CM5A_MR ("dry" climate model)
  - MRI_CGCM3 ("least warm" climate model)
  - NorESM1_M ("middle" climate model)

- Emissions and Socioeconomic Pathways:
  - rcp45_ssp1: Low emissions forcing, medium growth
  - rcp85_ssp2: High emissions forcing, medium growth
  - rcp85_ssp3: High emissions forcing, low growth
  - rcp85_ssp5: High emissions forcing, high growth

### Time Periods
- Calibration period: 2012-2020
- Projection periods: 2020-2070 in 10-year intervals
  - 2020-2030
  - 2030-2040
  - 2040-2050
  - 2050-2060
  - 2060-2070

### Land Use Categories
Transitions between five main land use types:
- Cropland
- Pasture land
- Rangeland
- Forest land
- Urban developed land

### Geographic Coverage
- All counties in the conterminous United States
- Counties identified by 5-digit FIPS codes

## Data Processing Pipeline

1. Raw Data (`data/raw/`)
   - JSON format: `county_landuse_projections_RPA.json`
   - Units: Land area in hundreds of acres
   - Includes metadata and variable descriptions

2. Conversion to Parquet
   - Script: `data/scripts/convert_json_to_parquet.py`
   - Converts JSON to columnar Parquet format for efficient processing
   - Output: `rpa_landuse_data.parquet`

3. SQLite Database
   - Structured tables for scenarios, time steps, counties, and land use transitions
   - Optimized for querying and analysis
   - Total records: ~5.4 million land use transitions

## Installation

1. Install system dependencies:
```bash
# On Ubuntu/Debian
sudo apt-get update
sudo apt-get install redis-server redis-tools

# On macOS
brew install redis

# On Windows
# Download the Redis installer from https://github.com/microsoftarchive/redis/releases
```

2. Start Redis server:
```bash
# On Ubuntu/Debian/macOS
sudo systemctl start redis-server  # Or: brew services start redis
redis-cli ping  # Should return PONG

# On Windows
# Redis will start automatically as a Windows service
```

3. Create and activate a Python virtual environment:
```bash
# Using conda (recommended)
conda create -n rpa_landuse python=3.12
conda activate rpa_landuse

# OR using venv
python -m venv .venv
source .venv/bin/activate  # On Linux/Mac
# .venv\Scripts\activate  # On Windows
```

2. Install the package in development mode:
```bash
pip install -e .
```

Required dependencies:
- FastAPI: REST API framework
- Pandas: Data processing and analysis
- SQLite3: Database operations (built into Python)
- Redis: Caching layer
- PyArrow: Parquet file handling
- Testing tools (pytest)

## Database Setup

The project requires Redis for caching:

1. Start Redis (if not already running):
```bash
sudo systemctl start redis-server
redis-cli ping  # Verify Redis is running
```

Redis configuration:
- Host: localhost
- Port: 6379
- No authentication required for development
- Used for API response caching

The SQLite database will be automatically created in the `data/database` directory when you run the data loading script.

## Data Loading

1. Convert JSON to Parquet:
```bash
cd data/scripts
python convert_json_to_parquet.py
```

2. Load data into SQLite:
```bash
python -m src.data_setup.db_loader
```

This will:
- Create the SQLite database file at `data/database/rpa_landuse.db` if it doesn't exist
- Initialize the database schema
- Load the data from the Parquet file into the database

## Working with Existing Database

If you're joining the project with an existing database setup:

1. Set up the Python environment:
```bash
# Using conda
conda create -n rpa_landuse python=3.12
conda activate rpa_landuse

# OR using venv
python -m venv .venv
source .venv/bin/activate  # On Linux/Mac
# .venv\Scripts\activate  # On Windows

pip install -e .
```

2. Verify database file exists:
```bash
# Check if SQLite database file exists
ls -l data/database/rpa_landuse.db
```

3. Verify data availability:
```bash
# Connect to SQLite and check record count
sqlite3 data/database/rpa_landuse.db "SELECT COUNT(*) FROM land_use_transitions;"
```

4. Start the development server:
```bash
# From the project root
uvicorn app.main:app --reload --port 8000
```

The API will be available at:
- API endpoints: http://localhost:8000/
- Interactive documentation: http://localhost:8000/docs
- Alternative documentation: http://localhost:8000/redoc

### Common Development Tasks

1. Query the database:
```python
from src.api.database import DatabaseConnection

# Example: Get all scenarios
conn = DatabaseConnection.get_connection()
cursor = conn.cursor()
cursor.execute("SELECT * FROM scenarios")
scenarios = cursor.fetchall()
```

2. Run specific test categories:
```bash
# Run only API tests
pytest tests/test_api.py -v

# Run tests with print statements
pytest -v -s

# Run tests matching a pattern
pytest -v -k "test_scenario"
```

3. Check API endpoints:
```bash
# Get available scenarios
curl http://localhost:8000/scenarios

# Get transitions for a specific county
curl http://localhost:8000/transitions?fips=01001&scenario=CNRM_CM5_rcp45_ssp1
```

4. Development utilities:
```bash
# Format code
black .

# Check type hints
mypy .

# Run linter
flake8
```

## Development

### Running Tests

Tests are organized in order of dependency:
1. Validation tests: `pytest tests/test_validation.py`
2. Basic API tests: `pytest tests/test_api.py`
3. API endpoint tests: `pytest tests/test_api_endpoints.py`
4. Data loader tests: `pytest tests/test_data_loader.py`

Run all tests:
```bash
pytest -v
```

### Database Schema

The SQLite database includes the following tables:

1. `scenarios`
   - scenario_id (PK)
   - scenario_name
   - gcm (Global Climate Model)
   - rcp (Representative Concentration Pathway)
   - ssp (Shared Socioeconomic Pathway)

2. `time_steps`
   - time_step_id (PK)
   - start_year
   - end_year

3. `counties`
   - fips_code (PK)
   - county_name

4. `land_use_transitions`
   - transition_id (PK)
   - scenario_id (FK)
   - time_step_id (FK)
   - fips_code (FK)
   - from_land_use
   - to_land_use
   - acres

## Data Source

This dataset was developed by the USDA Forest Service for the Resources Planning Act (RPA) 2020 Assessment. For more information, visit: https://doi.org/10.2737/RDS-2023-0026

## License

[Add appropriate license information]