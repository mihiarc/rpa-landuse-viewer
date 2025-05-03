#!/bin/bash
# Script to add the ensemble scenario to the RPA Land Use database

set -e  # Exit on error

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Virtual environment not found. Running setup_venv.sh..."
    ./setup_venv.sh
fi

# Activate virtual environment
source .venv/bin/activate

# Run the ensemble scenario creation script
echo "Creating ensemble scenario..."
python add_ensemble_scenario.py

echo "Ensemble scenario process complete!" 