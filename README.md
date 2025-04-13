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

### Data Source

This dataset was developed by Mihiar, Lewis & Coulston for the USDA Forest Service for the Resources Planning Act (RPA) 2020 Assessment. Download the data here: https://doi.org/10.2737/RDS-2023-0026. Unzip the .json data file to data/raw/. 

1. Raw Data (`data/raw/`)
   - JSON format: `county_landuse_projections_RPA.json`
   - Units: Land area in hundreds of acres
   - See _variable_descriptions.csv for data dictionary

2. Conversion to Parquet
   - Script: `src/data_setup/converter.py`
   - Converts JSON to columnar Parquet format for efficient processing
   - Output: `data/processed/rpa_landuse_data.parquet`

3. SQLite Database
   - Structured tables for scenarios, time steps, counties, and land use transitions
   - Optimized for querying and analysis
   - Total records: ~5.4 million land use transitions

## Installation

1. Create and activate a Python virtual environment:
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
- Pandas: Data processing and analysis
- SQLite3: Database operations (built into Python)
- PyArrow: Parquet file handling
- Testing tools (pytest)

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