#!/bin/bash
# Setup script for RPA Land Use Viewer development environment
# Uses pip for package management

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Setting up development environment for RPA Land Use Viewer${NC}"

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
    python3.11 -m venv .venv
fi

# Activate virtual environment
echo -e "${GREEN}Activating virtual environment...${NC}"
source .venv/bin/activate

# Ensure OpenBLAS is installed (required for scipy)
echo -e "${GREEN}Checking for OpenBLAS...${NC}"
if ! brew list openblas &>/dev/null; then
    echo -e "${YELLOW}Installing OpenBLAS via Homebrew (required for scipy)...${NC}"
    brew install openblas
fi

# Set environment variables for building scientific packages
echo -e "${GREEN}Setting up build environment for scientific packages...${NC}"
export OPENBLAS="$(brew --prefix openblas)"
export CFLAGS="-I$OPENBLAS/include"
export LDFLAGS="-L$OPENBLAS/lib"

# Install dependencies
echo -e "${GREEN}Installing packages with pip...${NC}"
pip install -r requirements.txt

# Get Python version from the virtual environment
VENV_PYTHON_VERSION=$(python --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo -e "${GREEN}Using Python version: ${VENV_PYTHON_VERSION}${NC}"

# Final message
echo -e "${GREEN}Setup complete!${NC}" 