#!/bin/bash

# Script to run the RPA Land Use tests in a virtual environment using uv
# Requirements: uv package manager (install via pip install uv)

# Stop on any error
set -e

echo "Setting up virtual environment for RPA Land Use tests..."

# Create virtual environment if it doesn't exist
if [ ! -d ".venv-test" ]; then
    echo "Creating new virtual environment with uv using Python 3.11..."
    uv venv --python=3.11 .venv-test
fi

# Activate virtual environment
source .venv-test/bin/activate

# Install dependencies with uv from requirements.txt
echo "Installing dependencies with uv from requirements.txt..."
uv pip install -r requirements.txt

# Install test dependencies
echo "Installing test dependencies..."
uv pip install pytest

# Install the package in development mode
echo "Installing rpa-landuse-viewer in development mode..."
uv pip install -e .

# Run the tests
echo "Running tests..."
python -m pytest tests/test_common_queries.py -v

echo "Tests completed." 