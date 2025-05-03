#!/bin/bash
# Script to add SSP ensemble scenarios to the RPA Land Use database

set -e  # Exit on error

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Virtual environment not found. Running setup_venv.sh..."
    ./setup_venv.sh
fi

# Activate virtual environment
source .venv/bin/activate

# Run the SSP ensemble scenario creation script
echo "Creating SSP ensemble scenarios..."
python add_ssp_ensemble_scenarios.py

echo "SSP ensemble scenario process complete!" 