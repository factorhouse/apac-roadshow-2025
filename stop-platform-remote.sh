#!/bin/bash

# ==============================================================================
# Shutdown Script: Stop All KartShoppe Services
# ==============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}"
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                                                                ║"
echo "║           🛑  Stopping KartShoppe Platform  🛑                 ║"
echo "║                                                                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo ""

# Stop Frontend (port 3000)
echo -e "${YELLOW}Stopping Frontend...${NC}"
if lsof -ti:3000 >/dev/null 2>&1; then
    lsof -ti:3000 | xargs kill -9 2>/dev/null || true
    echo -e "${GREEN}✓${NC} Frontend stopped"
else
    echo -e "${BLUE}ℹ${NC} Frontend not running"
fi

# Stop Quarkus API (port 8080)
echo -e "${YELLOW}Stopping Quarkus API...${NC}"
if lsof -ti:8080 >/dev/null 2>&1; then
    lsof -ti:8080 | xargs kill -9 2>/dev/null || true
    echo -e "${GREEN}✓${NC} Quarkus API stopped"
else
    echo -e "${BLUE}ℹ${NC} Quarkus API not running"
fi

# Kill any running Gradle daemons
# echo -e "${YELLOW}Stopping Gradle daemons...${NC}"
# ./gradlew --stop 2>/dev/null || true
# echo -e "${GREEN}✓${NC} Gradle daemons stopped"

echo -e "${YELLOW}Stopping Gradle daemons...${NC}"
# pkill is a fast and efficient way to kill processes by name
if pkill -f gradle >/dev/null 2>&1; then
    echo -e "${GREEN}✓${NC} Gradle daemons stopped"
else
    echo -e "${BLUE}ℹ${NC} No Gradle daemons were running"
fi

# Stop Docker Compose services
echo -e "${YELLOW}Stopping Docker services...${NC}"
docker compose -f compose-remote.yml down -v
echo -e "${GREEN}✓${NC} Docker services stopped"

# Clean up PID files
if [ -d ".pids" ]; then
    rm -rf .pids
fi

# Clean up log files (optional)
if [ -f "/tmp/quarkus.log" ]; then
    rm -f /tmp/quarkus.log
fi
if [ -f "/tmp/quarkus.pid" ]; then
    rm -f /tmp/quarkus.pid
fi

# Clean up PostgreSQL CDC Replication Slot
echo -e "${YELLOW}Cleaning up CDC replication slot...${NC}"
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    python scripts/manage_db.py --action clean-cdc
    deactivate
    echo -e "${GREEN}✓${NC} CDC cleanup complete"
else
    echo -e "${YELLOW}Skipping CDC cleanup: Python virtual environment 'venv' not found.${NC}"
fi

echo ""
echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║              All services stopped successfully!                ║${NC}"
echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}To start again, run:${NC} ${GREEN}./start-platform-remote.sh${NC}"
echo ""
