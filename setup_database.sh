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
    python -m src.data_setup.import_landuse_data
else
    echo "Warning: Raw data file not found at data/raw/county_landuse_projections_RPA.json"
    echo "Database initialized without data. Run import manually when data is available."
fi

echo "Database setup complete!" 