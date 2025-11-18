#!/bin/bash

echo "🧹 Cleaning KartShoppe Environment"
echo "===================================="

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Stop all running services
echo -e "\n${BLUE}Stopping all services...${NC}"
./stop-all.sh 2>/dev/null || true

# Clean Docker environment
echo -e "\n${BLUE}Cleaning Docker environment...${NC}"

# Remove orphaned containers
echo "Removing orphaned containers..."
docker ps -aq --filter "name=ververica" | xargs -r docker rm -f 2>/dev/null
docker ps -aq --filter "name=kafka" | xargs -r docker rm -f 2>/dev/null
docker ps -aq --filter "name=redpanda" | xargs -r docker rm -f 2>/dev/null

# Clean volumes (optional, commented out by default)
# echo "Removing Docker volumes..."
# docker volume prune -f

echo -e "${GREEN}✓ Docker environment cleaned${NC}"

# Clean build artifacts
echo -e "\n${BLUE}Cleaning build artifacts...${NC}"
./gradlew clean 2>/dev/null || true
rm -rf .gradle/
rm -rf */build/
rm -rf logs/
rm -rf .pids/

echo -e "${GREEN}✓ Build artifacts cleaned${NC}"

# Clean frontend artifacts  
echo -e "\n${BLUE}Cleaning frontend artifacts...${NC}"
rm -rf kartshoppe-frontend/dist/
rm -rf kartshoppe-frontend/.parcel-cache/

echo -e "${GREEN}✓ Frontend artifacts cleaned${NC}"

echo -e "\n${GREEN}========================================"
echo -e "✅ Environment cleaned successfully!"
echo -e "========================================${NC}"
echo
echo -e "To start fresh: ${YELLOW}./start-platform-local.sh${NC} or ${YELLOW}./start-platform-managed.sh${NC}"
