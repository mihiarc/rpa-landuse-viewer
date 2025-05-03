#!/bin/bash

# Colors for better output
GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[0;33m"
BLUE="\033[0;34m"
NC="\033[0m" # No Color

# Print header
echo -e "${BLUE}====================================${NC}"
echo -e "${BLUE}  RPA Land Use Data Validation Tests ${NC}"
echo -e "${BLUE}====================================${NC}"

# Process command line arguments
VERBOSE=0
SPECIFIC_TEST=""
QUICK=0

for arg in "$@"; do
  case $arg in
    -v|--verbose)
      VERBOSE=1
      shift
      ;;
    -q|--quick)
      QUICK=1
      shift
      ;;
    -f|--forest)
      SPECIFIC_TEST="TestForestLand"
      shift
      ;;
    -d|--developed)
      SPECIFIC_TEST="TestDevelopedLand"
      shift
      ;;
    -c|--crop)
      SPECIFIC_TEST="TestCropLand"
      shift
      ;;
    -p|--pasture)
      SPECIFIC_TEST="TestPastureLand"
      shift
      ;;
    -r|--rangeland)
      SPECIFIC_TEST="TestRangeland"
      shift
      ;;
  esac
done

# Activate the virtual environment
if [ -d ".venv/bin" ]; then
    echo -e "${BLUE}Activating virtual environment...${NC}"
    source .venv/bin/activate
else
    echo -e "${YELLOW}Virtual environment not found at .venv/bin.${NC}"
    echo -e "${YELLOW}Using system Python.${NC}"
fi

# Print Python and package versions
echo -e "${BLUE}Using Python:${NC} $(python --version)"
if [ $VERBOSE -eq 1 ]; then
    echo -e "${BLUE}Package versions:${NC}"
    pip list | grep -E "pytest|duckdb|pandas"
fi

# Ensure we're using the correct Python with src in path
export PYTHONPATH=$PYTHONPATH:$(pwd)

# Build the pytest command
PYTEST_CMD="python -m pytest"

# Add verbosity based on arguments
if [ $VERBOSE -eq 1 ]; then
    PYTEST_CMD="$PYTEST_CMD -vv"
else
    PYTEST_CMD="$PYTEST_CMD -v"
fi

# Add specific test if specified
if [ -n "$SPECIFIC_TEST" ]; then
    echo -e "${BLUE}Running only $SPECIFIC_TEST tests${NC}"
    PYTEST_CMD="$PYTEST_CMD tests/test_landuse_data.py::$SPECIFIC_TEST"
else
    PYTEST_CMD="$PYTEST_CMD tests/"
fi

# Add quick mode which runs a subset of tests
if [ $QUICK -eq 1 ]; then
    echo -e "${BLUE}Running in quick mode (only testing 'Least warm' climate projection)${NC}"
    PYTEST_CMD="$PYTEST_CMD -k \"Least\""
fi

# Print the command that will be run
if [ $VERBOSE -eq 1 ]; then
    echo -e "${BLUE}Running command:${NC} $PYTEST_CMD"
fi

# Run the tests
echo -e "${BLUE}Running tests...${NC}"
eval $PYTEST_CMD

# Check the result
if [ $? -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed.${NC}"
    exit 1
fi 