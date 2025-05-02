#!/bin/bash
# Setup script for RPA Land Use Viewer development environment
# Uses UV for fast package management

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Setting up development environment for RPA Land Use Viewer${NC}"

# Check if UV is installed
if ! command -v uv &> /dev/null; then
    echo -e "${RED}UV is not installed. Please install UV first:${NC}"
    echo -e "${YELLOW}pip install uv${NC} or ${YELLOW}brew install uv${NC}"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo -e "${GREEN}Creating virtual environment...${NC}"
    uv venv .venv
else
    echo -e "${YELLOW}Virtual environment already exists.${NC}"
fi

# Activate virtual environment
echo -e "${GREEN}Activating virtual environment...${NC}"
source .venv/bin/activate

# Install dependencies
echo -e "${GREEN}Installing dependencies...${NC}"

# Create requirements.txt if it doesn't exist
if [ ! -f "requirements.txt" ]; then
    echo -e "${YELLOW}Creating requirements.txt with core dependencies...${NC}"
    cat > requirements.txt << EOF
duckdb>=0.10.0
pandas>=2.1.0
numpy>=1.26.0
python-dotenv>=1.0.0
pyarrow>=15.0.0
pytest>=7.4.0
flake8>=6.1.0
black>=23.9.0
EOF
fi

# Install packages with UV
echo -e "${GREEN}Installing packages with UV...${NC}"
uv pip install -r requirements.txt

# Install development package in editable mode
echo -e "${GREEN}Installing package in development mode...${NC}"
uv pip install -e .

# Final message
echo -e "${GREEN}Setup complete!${NC}"
echo -e "To activate the environment, run: ${YELLOW}source .venv/bin/activate${NC}"
echo -e "To optimize the database, run: ${YELLOW}python src/db/optimize_database.py --all${NC}"
echo -e "To initialize a new database, run: ${YELLOW}python src/db/initialize_database.py --with-migrations${NC}" 