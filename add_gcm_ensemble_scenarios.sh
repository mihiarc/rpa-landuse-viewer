#!/bin/bash
# Script to add GCM ensemble scenarios to the RPA Land Use database

set -e  # Exit on error

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Virtual environment not found. Running setup_venv.sh..."
    ./setup_venv.sh
fi

# Activate virtual environment
source .venv/bin/activate

# Run the GCM ensemble scenario creation script
echo "Creating GCM ensemble scenarios..."
python add_gcm_ensemble_scenarios.py

echo "GCM ensemble scenario process complete!" 