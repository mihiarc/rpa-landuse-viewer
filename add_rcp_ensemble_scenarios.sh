#!/bin/bash
# Script to add RCP ensemble scenarios to the RPA Land Use database

set -e  # Exit on error

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Virtual environment not found. Running setup_venv.sh..."
    ./setup_venv.sh
fi

# Activate virtual environment
source .venv/bin/activate

# Run the RCP ensemble scenario creation script
echo "Creating RCP ensemble scenarios..."
python add_rcp_ensemble_scenarios.py

echo "RCP ensemble scenario process complete!" 