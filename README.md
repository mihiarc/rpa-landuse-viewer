# RPA Land Use Change Data Processing

This repository contains tools for processing and analyzing RPA (Resources Planning Act) land use change projection data. The data represents land use transitions across different scenarios, years, and counties in the United States.

## Data Structure

The original data is stored in an R data file (RDS format) with the following nested structure:

- Scenarios (multiple climate-socioeconomic scenarios)
  - Time steps (years from 2020 onwards in 5-year intervals)
    - Counties (identified by FIPS codes)
      - Transition matrices (6x6) showing land use changes between categories:
        - Crop
        - Pasture
        - Range
        - Forest
        - Urban
        - Other

The data is converted to a Parquet file with a flat structure containing the following columns:

- `Scenario`: The scenario identifier
- `Year`: The projection year
- `FIPS`: County FIPS code
- `From`: Original land use category
- `To`: Destination land use category
- `Acres`: Amount of land transitioning (in acres)

## Setup

1. Create a Python environment:
```bash
conda create -n rpa_landuse python=3.9
conda activate rpa_landuse
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Converting RDS to Parquet

To convert the original RDS file to Parquet format:

```bash
python convert_rds_to_parquet.py
```

This will create a file named `rpa_landuse_data.parquet` containing the processed data.

### Loading and Analyzing Data

The `data_loader.py` module provides functions for working with the converted data:

```python
from data_loader import load_landuse_data, filter_data, get_unique_values, summarize_transitions

# Load the data
df = load_landuse_data()

# Filter data for specific criteria
filtered_df = filter_data(
    df,
    scenario='scenario_1',
    year=2020,
    fips='01001'
)

# Get unique values from a column
scenarios = get_unique_values(df, 'Scenario')

# Summarize transitions
summary = summarize_transitions(df, group_by=['Scenario', 'Year'])
```

## Testing

Run the unit tests using pytest:

```bash
pytest test_data_loader.py
```

## Data Files

- `RDS-2023-0026/Data/county_landuse_projections_RPA.rds`: Original R data file
- `rpa_landuse_data.parquet`: Converted data in Parquet format

## Notes

- The data conversion process preserves all information from the original RDS file while making it more accessible for Python-based analysis.
- The Parquet format provides efficient storage and fast querying capabilities.
- All functions include type hints and documentation for better code maintainability. 