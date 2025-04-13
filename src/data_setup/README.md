# Data Setup for RPA Land Use Viewer

This module provides simplified tools for one-time data processing and database setup for the RPA Land Use Viewer application.

## Overview

The data setup process involves three main steps:

1. **Conversion**: Convert the raw JSON data to Parquet format
2. **Validation**: Perform basic validation checks on the converted data
3. **Database Loading**: Load the data into MySQL for application use

## Quick Start

The simplest way to run the complete setup process is:

```bash
# From the project root directory
python -m src.data_setup.setup
```

This will:
- Convert the JSON data from the default location
- Validate the converted data
- Load the data into MySQL
- Verify the database loading

## Module Components

### 1. Converter (converter.py)

Converts the raw JSON data to Parquet format.

```bash
# Standalone usage
python -m src.data_setup.converter --input data/raw/county_landuse_projections_RPA.json --output data/processed/rpa_landuse_data.parquet
```

### 2. Validator (validator.py)

Performs basic validation on the Parquet data.

```bash
# Standalone usage
python -m src.data_setup.validator --input data/processed/rpa_landuse_data.parquet
```

### 3. Database Loader (db_loader.py)

Loads the Parquet data into the MySQL database.

```bash
# Standalone usage
python -m src.data_setup.db_loader --input data/processed/rpa_landuse_data.parquet --verify
```

## Advanced Usage

### Custom Paths

You can specify custom file paths:

```bash
python -m src.data_setup.setup --json /path/to/input.json --parquet /path/to/output.parquet
```

### Skipping Steps

You can skip validation or database loading:

```bash
# Skip validation
python -m src.data_setup.setup --skip-validation

# Skip database loading
python -m src.data_setup.setup --skip-db-load

# Only perform conversion
python -m src.data_setup.setup --skip-validation --skip-db-load
```

### Environment Variables

Database connection can be customized with environment variables:

```bash
# Set custom database connection
export MYSQL_HOST=custom-host
export MYSQL_USER=custom-user
export MYSQL_PASSWORD=custom-password
export MYSQL_DATABASE=custom-db

# Run the setup
python -m src.data_setup.setup
```

## Troubleshooting

### Common Issues

1. **File not found errors**:
   - Ensure the input JSON file exists at the specified path
   - Ensure you have proper permissions to read/write files

2. **Database connection issues**:
   - Verify MySQL is running
   - Check credentials and connection details
   - Ensure the database exists

3. **Memory issues during conversion**:
   - The conversion process can be memory-intensive
   - Try closing other applications to free up memory

### Logs

The setup process creates a `data_setup.log` file with detailed information about the setup process. 