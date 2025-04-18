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

### Pipeline Overview

```mermaid
flowchart TD
    A[Raw Data: JSON] -->|src/data_setup/converter.py| B[Processed Data: Parquet]
    B -->|src/data_setup/validator.py| C{Validation}
    C -->|Valid| D[SQLite Database]
    C -->|Invalid| E[Error Handling]
    D -->|src/db/queries.py| F[Data Queries]
    F --> G[Streamlit App]
```

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
# Using UV (recommended)
uv venv .venv-py311 --python 3.11
source .venv-py311/bin/activate  # On Linux/Mac
# .venv-py311\Scripts\activate  # On Windows

# OR using conda
conda create -n rpa_landuse python=3.11
conda activate rpa_landuse
```

2. Install the package in development mode:
```bash
# Using UV (recommended)
uv pip install -r requirements.txt

# OR using pip
pip install -e .
```

Required dependencies:
- Pandas: Data processing and analysis
- SQLite3: Database operations (built into Python)
- PyArrow: Parquet file handling
- NumPy: <2.0.0 for compatibility with pandas 1.5.3

## Data Loading

1. Convert JSON to Parquet:
```bash
python -m src.data_setup.converter
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
# Using UV (recommended)
uv venv .venv-py311 --python 3.11
source .venv-py311/bin/activate  # On Linux/Mac
# .venv-py311\Scripts\activate  # On Windows

# Install requirements
uv pip install -r requirements.txt
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

```mermaid
erDiagram
    SCENARIOS {
        int scenario_id PK
        string scenario_name
        string gcm
        string rcp
        string ssp
    }
    TIME_STEPS {
        int time_step_id PK
        int start_year
        int end_year
    }
    COUNTIES {
        string fips_code PK
        string county_name
    }
    LAND_USE_TRANSITIONS {
        int transition_id PK
        int scenario_id FK
        int time_step_id FK
        string fips_code FK
        string from_land_use
        string to_land_use
        float acres
    }
    
    SCENARIOS ||--o{ LAND_USE_TRANSITIONS : "has"
    TIME_STEPS ||--o{ LAND_USE_TRANSITIONS : "contains"
    COUNTIES ||--o{ LAND_USE_TRANSITIONS : "includes"
```

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

## Streamlit Dashboard App

### Application Architecture

```mermaid
flowchart TD
    A[Home.py] --> B[Main Dashboard]
    A --> C[Pages]
    
    B --> D[Data Filtering]
    D --> D1[County Selection]
    D --> D2[Time Period Selection]
    D --> D3[Land Use Type Selection]
    
    B --> E[Data Visualization]
    E --> E1[Data Tables]
    E --> E2[Charts/Graphs]
    E --> E3[Maps]
    
    B --> F[AI Analysis]
    F --> F1[PandasAI Integration]
    F --> F2[Natural Language Queries]
    
    C --> G[County Explorer]
    C --> H[Comparison Tool]
    C --> I[Projection Analysis]
```

A Streamlit-based web application is provided for interactive visualization and analysis of the land use change data. The app features:

1. Data filtering by:
   - County
   - Time period (start and end years)
   - Land use types

2. Multiple view options:
   - Data tables with land use transition details
   - Statistical summaries of land changes
   - Map visualizations

3. AI-powered analysis:
   - Natural language querying of the data using PandasAI
   - Ask questions about trends, patterns, and statistical information

### Running the Dashboard

1. Set up environment variables:
```bash
# Copy the example environment file
cp .env.example .env

# Edit the .env file to add your OpenAI API key
nano .env  # or use any text editor
```

2. Run the application:
```bash
# Using the provided script (Linux/Mac)
./run_with_py311.sh

# OR manually
streamlit run Home.py
```

3. Open your browser to http://localhost:8501

### Example Queries for PandasAI

Once the app is running, you can ask questions about the data such as:
- "What land use type lost the most acreage?"
- "What's the top destination for converted forest land?"
- "Show me a bar chart of acres by land use type"
- "Calculate the percentage change for each land use type"

## Geographic Data Hierarchy

The RPA Land Use Viewer includes a hierarchical geographic data structure that organizes spatial data across multiple administrative levels:

### Geographic Levels

1. **Counties**: The base level of geographic data, using 5-digit FIPS codes
2. **States**: Groups counties by state using the first 2 digits of the county FIPS code
3. **Sub-regions**: (Future implementation) Will group states into logical geographical regions
4. **Regions**: (Future implementation) Will group sub-regions into major regions

### Implementation Details

The database schema implements this hierarchy using:

- **Direct Relationships**: Counties are linked to states via the FIPS code prefix (first 2 digits)
- **Recursive Common Table Expression (CTE)**: The `region_hierarchy` view uses a recursive CTE to navigate the geographic hierarchy from counties up to states (and later to regions)
- **Views**: Multiple database views provide aggregated data at different geographic levels

### Key Features

- **Flexible Filtering**: The UI allows filtering by national, state, or county levels
- **Aggregation**: Data can be viewed at any level of the hierarchy with appropriate aggregation
- **Relational Integrity**: Foreign key constraints and indexes ensure data consistency across levels
- **Extendable**: The schema is designed to easily accommodate additional hierarchical levels

### Example Usage

```sql
-- Get all counties in a state
SELECT * FROM counties_by_state WHERE state_fips = '06';

-- Get aggregated land use data at the state level
SELECT * FROM state_land_use_summary 
WHERE scenario_id = 1 AND state_fips = '06';

-- Use the hierarchical view to traverse the geography tree
SELECT * FROM region_hierarchy 
WHERE parent_id = '06' AND region_type = 'COUNTY';
```