#!/bin/bash
# Script to add ensemble scenarios to the RPA Land Use database

set -e  # Exit on error

# Terminal colors and formatting
GREEN="\033[0;32m"
YELLOW="\033[0;33m"
CYAN="\033[0;36m"
BLUE="\033[0;34m"
RED="\033[0;31m"
BOLD="\033[1m"
RESET="\033[0m"

# Help message
function show_help {
  echo -e "${BOLD}${BLUE}Usage:${RESET} $0 [OPTIONS]"
  echo -e "${BOLD}Add ensemble scenarios to the RPA Land Use database${RESET}"
  echo ""
  echo -e "${BOLD}${BLUE}Options:${RESET}"
  echo -e "  ${CYAN}--all${RESET}         Create all ensemble scenarios (default)"
  echo -e "  ${CYAN}--integrated${RESET}  Create only the four integrated RPA scenario ensembles (LM, HL, HM, HH)"
  echo -e "  ${CYAN}--overall${RESET}     Create only the overall mean ensemble across all scenarios"
  echo -e "  ${CYAN}--force${RESET}       Force recreation without confirmation prompts"
  echo -e "  ${CYAN}-h, --help${RESET}    Show this help message"
}

# Parse command line arguments
OPTIONS=""
if [ $# -eq 0 ]; then
  # Default to --all if no arguments provided
  OPTIONS="--all"
else
  for arg in "$@"; do
    case $arg in
      -h|--help)
        show_help
        exit 0
        ;;
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

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo -e "${YELLOW}Virtual environment not found. Running setup_venv.sh...${RESET}"
    ./setup_venv.sh
fi

# Activate virtual environment
source .venv/bin/activate

# Run the ensemble scenarios creation script
echo -e "${BOLD}${GREEN}Creating ensemble scenarios with options:${RESET} ${CYAN}$OPTIONS${RESET}"
python -m src.db.add_ensemble_scenario $OPTIONS

echo -e "${BOLD}${GREEN}Ensemble scenarios process complete!${RESET}" 