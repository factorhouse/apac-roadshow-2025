#!/bin/bash
#
# Common environment loader for KartShoppe training scripts.
# This script should be 'sourced' by other scripts, not executed directly.

# Define color codes for consistent output in all scripts
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

#
# FUNCTION: load_and_print_env
#
# Loads variables from a .env file into the environment and prints
# a summary, hiding any values for keys that end in 'PASSWORD'.
# Exits if .env is not found.
#
load_and_print_env() {
  if [ ! -f .env ]; then
    echo -e "${RED}✗ Configuration file .env not found!${NC}"
    echo -e "${YELLOW}Create .env either from .env.local or .env.managed first.${NC}"
    exit 1
  fi

  # Export variables from .env, ignoring comments and blank lines
  export $(grep -v -e '^#' -e '^$' .env | xargs)

  echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo -e "${CYAN}Active Configuration (passwords hidden):${NC}"
  # Read the file again to safely print the values
  grep -v -e '^#' -e '^$' .env | while IFS='=' read -r key value; do
    if [[ ! "$key" =~ PASSWORD$ ]]; then
      echo -e "  ${GREEN}▸${NC} ${key}: ${YELLOW}${value}${NC}"
    else
      echo -e "  ${GREEN}▸${NC} ${key}: ${YELLOW}********${NC}"
    fi
  done
  echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
  echo ""
}