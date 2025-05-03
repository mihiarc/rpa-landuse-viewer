#!/bin/bash
# Script to update counties table with state, region, and subregion information

# Determine the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Check if virtual environment exists, if not create one
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    uv venv .venv
fi

# Activate the virtual environment
source .venv/bin/activate

# Ensure required packages are installed
echo "Ensuring required packages are installed..."
uv pip install -r requirements.txt
uv pip install pyyaml

# First run update_county_names.py to fetch latest county data from Census API
echo "Fetching county data from Census API..."
python src/utils/update_county_names.py

# Check if the command was successful
if [ $? -ne 0 ]; then
    echo "County names update failed."
    exit 1
fi

# Then run the region update script (now in utils module)
echo "Running counties region update script..."
python src/utils/update_counties_regions.py

# Check if the command was successful
if [ $? -eq 0 ]; then
    echo "Counties update completed successfully."
else
    echo "Counties update failed."
    exit 1
fi

# Deactivate virtual environment
deactivate

echo "Update process completed." 