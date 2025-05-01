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

# Install the project with development dependencies using pyproject.toml
echo "Installing project with development dependencies..."
uv pip install -e ".[dev]"

# Run the tests with coverage reporting
echo "Running tests with coverage..."
# Use configuration from pyproject.toml for pytest and coverage
python -m pytest tests/ --cov=src --cov-report=term --cov-report=html:tests/coverage \
    --ignore=tests/benchmark_duckdb.py \
    --ignore=tests/generate_query_results.py \
    --ignore=dev/

echo "Tests completed."
echo "Coverage report available in tests/coverage directory." 