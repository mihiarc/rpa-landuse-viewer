#!/bin/bash
# RPA Land Use Viewer data management script
# Combined script for updating counties and adding ensemble scenarios

set -e  # Exit on error

# Terminal colors and formatting
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
CYAN="\033[0;36m"
BLUE="\033[0;34m"
RED="\033[0;31m"
BOLD="\033[1m"
RESET="\033[0m"

# Determine the script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Help message
function show_help {
  echo -e "${BOLD}${BLUE}Usage:${RESET} $0 [COMMAND] [OPTIONS]"
  echo -e "${BOLD}RPA Land Use Viewer data management script${RESET}"
  echo ""
  echo -e "${BOLD}${BLUE}Commands:${RESET}"
  echo -e "  ${CYAN}counties${RESET}      Update counties with state, region, and subregion information"
  echo -e "  ${CYAN}ensemble${RESET}      Add ensemble scenarios to the database"
  echo -e "  ${CYAN}all${RESET}           Run both counties update and ensemble scenarios"
  echo -e "  ${CYAN}-h, --help${RESET}    Show this help message"
  echo ""
  echo -e "${BOLD}${BLUE}Ensemble Options:${RESET}"
  echo -e "  ${CYAN}--all${RESET}         Create all ensemble scenarios (default)"
  echo -e "  ${CYAN}--integrated${RESET}  Create only the four integrated RPA scenario ensembles (LM, HL, HM, HH)"
  echo -e "  ${CYAN}--overall${RESET}     Create only the overall mean ensemble across all scenarios"
  echo -e "  ${CYAN}--force${RESET}       Force recreation without confirmation prompts"
}

# Function to setup and activate virtual environment
function setup_venv {
    # Check if virtual environment exists, if not create one
    if [ ! -d ".venv" ]; then
        echo -e "${YELLOW}Creating virtual environment...${RESET}"
        python3 -m venv .venv
    fi

    # Activate the virtual environment
    source .venv/bin/activate

    # Ensure required packages are installed
    echo -e "${YELLOW}Ensuring required packages are installed...${RESET}"
    pip install -r requirements.txt
    pip install pyyaml
}

# Function to update counties
function update_counties {
    echo -e "${BOLD}${GREEN}Starting counties update process...${RESET}"
    
    # First run update_county_names.py to fetch latest county data from Census API
    echo -e "${YELLOW}Fetching county data from Census API...${RESET}"
    python src/utils/update_county_names.py

    # Check if the command was successful
    if [ $? -ne 0 ]; then
        echo -e "${RED}County names update failed.${RESET}"
        return 1
    fi

    # Then run the region update script (now in utils module)
    echo -e "${YELLOW}Running counties region update script...${RESET}"
    python src/utils/update_counties_regions.py

    # Check if the command was successful
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}Counties update completed successfully.${RESET}"
    else
        echo -e "${RED}Counties update failed.${RESET}"
        return 1
    fi
    
    return 0
}

# Function to add ensemble scenarios
function add_ensemble_scenarios {
    local options="$1"
    
    echo -e "${BOLD}${GREEN}Creating ensemble scenarios with options:${RESET} ${CYAN}$options${RESET}"
    python -m src.db.add_ensemble_scenario $options
    
    if [ $? -eq 0 ]; then
        echo -e "${BOLD}${GREEN}Ensemble scenarios process complete!${RESET}"
    else
        echo -e "${RED}Ensemble scenarios process failed.${RESET}"
        return 1
    fi
    
    return 0
}

# Parse command line arguments
COMMAND=""
OPTIONS=""

# If no arguments provided, show help
if [ $# -eq 0 ]; then
    show_help
    exit 0
fi

# Parse the first argument as the command
case "$1" in
    counties)
        COMMAND="counties"
        shift
        ;;
    ensemble)
        COMMAND="ensemble"
        shift
        if [ $# -eq 0 ]; then
            # Default to --all if no ensemble options provided
            OPTIONS="--all"
        else
            # Parse remaining arguments as ensemble options
            for arg in "$@"; do
                case $arg in
                    --all|--integrated|--overall|--force)
                        OPTIONS="$OPTIONS $arg"
                        ;;
                    *)
                        echo -e "${RED}Error:${RESET} Unknown option: $arg"
                        show_help
                        exit 1
                        ;;
                esac
            done
        fi
        ;;
    all)
        COMMAND="all"
        shift
        # Parse remaining arguments as ensemble options
        for arg in "$@"; do
            case $arg in
                --all|--integrated|--overall|--force)
                    OPTIONS="$OPTIONS $arg"
                    ;;
                *)
                    echo -e "${RED}Error:${RESET} Unknown option: $arg"
                    show_help
                    exit 1
                    ;;
            esac
        done
        if [ -z "$OPTIONS" ]; then
            # Default to --all if no ensemble options provided
            OPTIONS="--all"
        fi
        ;;
    -h|--help)
        show_help
        exit 0
        ;;
    *)
        echo -e "${RED}Error:${RESET} Unknown command: $1"
        show_help
        exit 1
        ;;
esac

# Setup virtual environment
setup_venv

# Execute the requested command
case "$COMMAND" in
    counties)
        update_counties
        ;;
    ensemble)
        add_ensemble_scenarios "$OPTIONS"
        ;;
    all)
        update_counties && add_ensemble_scenarios "$OPTIONS"
        ;;
esac

# Deactivate virtual environment
deactivate

echo -e "${BOLD}${GREEN}All operations completed.${RESET}" 