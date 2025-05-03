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

# Check for existing environment
REBUILD_MODE=false
if [ -d ".venv" ]; then
    echo -e "${YELLOW}Virtual environment already exists.${NC}"
    read -p "Rebuild environment from scratch? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Removing existing environment...${NC}" 
        rm -rf .venv
        REBUILD_MODE=true
    else
        echo -e "${GREEN}Updating existing environment...${NC}"
    fi
fi

# Create virtual environment if it doesn't exist or was removed
if [ ! -d ".venv" ] || [ "$REBUILD_MODE" = true ]; then
    echo -e "${GREEN}Creating virtual environment with Python 3.11...${NC}"
    uv venv --python 3.11 .venv
fi

# Activate virtual environment
echo -e "${GREEN}Activating virtual environment...${NC}"
source .venv/bin/activate

# Install dependencies
echo -e "${GREEN}Installing packages with UV...${NC}"

# Get Python version from the virtual environment
VENV_PYTHON_VERSION=$(python --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo -e "${GREEN}Using Python version: ${VENV_PYTHON_VERSION}${NC}"

# Install packages from requirements.txt
if [[ "${VENV_PYTHON_VERSION}" == "3.12" ]]; then
    echo -e "${GREEN}Using Python 3.12 compatible packages${NC}"
    # Install numpy first to ensure compatibility
    uv pip install "numpy>=1.25.0,<2.0.0" --upgrade
    uv pip install -r requirements.txt --upgrade
else
    echo -e "${GREEN}Using standard package versions${NC}"
    uv pip install -r requirements.txt --upgrade
fi

# Install development package in editable mode
echo -e "${GREEN}Installing package in development mode...${NC}"
uv pip install -e .

# Final message
echo -e "${GREEN}Setup complete!${NC}"
echo -e "To activate the environment, run: ${YELLOW}source .venv/bin/activate${NC}"
echo -e "To create semantic layers, run: ${YELLOW}python create_semantic_layers.py${NC}"
echo -e "To query the semantic layers, run: ${YELLOW}python query_semantic_layers.py${NC}" 