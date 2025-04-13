#!/bin/bash

# Set up colors for terminal output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Setting up Python 3.11 environment for RPA Land Use Viewer...${NC}"

# Check if Python 3.11 is installed
if ! command -v python3.11 &> /dev/null; then
    echo -e "${RED}Python 3.11 is not installed. Please install Python 3.11 before running this script.${NC}"
    exit 1
fi

# Set up virtual environment if it doesn't exist
if [ ! -d ".venv-py311" ]; then
    echo -e "${YELLOW}Creating Python 3.11 virtual environment...${NC}"
    python3.11 -m venv .venv-py311
fi

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source .venv-py311/bin/activate

# Install requirements
echo -e "${YELLOW}Installing requirements...${NC}"
pip install -r requirements.txt

# Run the application
echo -e "${GREEN}Starting the application...${NC}"
streamlit run app.py

# Deactivate the virtual environment when done
deactivate 