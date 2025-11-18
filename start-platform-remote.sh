#!/bin/bash

# ==============================================================================
# KartShoppe Training Platform Startup
# ==============================================================================
# Starts the core platform WITHOUT any Flink jobs
# - Instaclustr Kafka Cluster
# - Instaclustr Postgres Instance
# - Quarkus API with integrated frontend (Quinoa)
# - Standalone Flink Cluster
# - Kpow and Flex
#
# Flink jobs are started separately during training modules
# ==============================================================================

set -e  # Exit on error

source env.sh

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

clear

echo -e "${CYAN}${BOLD}"
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                                                                ║"
echo "║        🛒  Starting KartShoppe Training Platform  🛒           ║"
echo "║                                                                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo ""

# Create directories for logs and PIDs
mkdir -p logs .pids

# Load and Print Environment Configuration from .env file
load_and_print_env

# ==============================================================================
# Step 1: Check Prerequisites
# ==============================================================================

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 1/5: Checking Prerequisites${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Check Docker
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}✗ Docker is not running${NC}"
    echo -e "${YELLOW}  Please start Docker Desktop and try again${NC}"
    exit 1
fi
echo -e "${GREEN}✓${NC} Docker is running"

# Check Java version
JAVA_VERSION=$(java -version 2>&1 | head -n 1 | sed 's/.*version "\([0-9]*\).*/\1/')
if [ "$JAVA_VERSION" -lt 17 ]; then
    echo -e "${YELLOW}⚠ Warning: Java 17+ is required for Quarkus${NC}"
    echo "  Current version: $(java -version 2>&1 | head -n 1)"
    echo ""
    echo -e "${YELLOW}  Switch to Java 17 with SDKMAN:${NC}"
    echo -e "    ${CYAN}sdk use java 17.0.13-tem${NC}"
    echo ""
    exit 1
fi
echo -e "${GREEN}✓${NC} Java 17+ detected: $(java -version 2>&1 | head -1 | awk '{print $3}' | tr -d '\"')"

# Check Node.js
if ! command -v node &> /dev/null; then
    echo -e "${RED}✗ Node.js is not installed${NC}"
    exit 1
fi
NODE_VERSION=$(node --version)
echo -e "${GREEN}✓${NC} Node.js ${NODE_VERSION}"

echo ""

# ==============================================================================
# Step 2: Initialize Postgres Instance
# ==============================================================================

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 2/5: Starting Infrastructure${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

## INIT DB

echo -e "${YELLOW}Starting Kpow, Flex, and Flink...${NC}"
docker compose -f compose-remote.yml up -d kpow flex jobmanager taskmanager

# ==============================================================================
# Step 3: Create Kafka Topics
# ==============================================================================

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 3/5: Creating Kafka Topics${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

## CREATE TOPICS

# ==============================================================================
# Step 4: Prepare Frontend
# ==============================================================================

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 4/5: Preparing Frontend${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Install frontend dependencies if needed (Quinoa will use these)
if [ ! -d "kartshoppe-frontend/node_modules" ]; then
    echo -e "${YELLOW}Installing frontend dependencies (required for Quinoa)...${NC}"
    cd kartshoppe-frontend
    npm install
    cd ..
    echo -e "${GREEN}✓${NC} Frontend dependencies installed"
else
    echo -e "${GREEN}✓${NC} Frontend dependencies already installed"
fi

# Build required modules
echo -e "${YELLOW}Building models module...${NC}"
./gradlew :models:build -q
echo -e "${GREEN}✓${NC} Models module built"

echo ""

# ==============================================================================
# Step 5: Start Quarkus with Integrated Frontend (Quinoa)
# ==============================================================================

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Step 5/5: Starting Quarkus API + Frontend (Quinoa)${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

echo -e "${YELLOW}Starting Quarkus in development mode...${NC}"
echo -e "${CYAN}(Frontend will be integrated via Quinoa)${NC}"
echo ""

# Start Quarkus in background, redirect output to log file
./gradlew :quarkus-api:quarkusDev > logs/quarkus.log 2>&1 &
QUARKUS_PID=$!
echo $QUARKUS_PID > .pids/quarkus.pid

echo -e "${YELLOW}Waiting for Quarkus to start (this may take 30-60 seconds)...${NC}"

# Wait for health check
for i in {1..40}; do
    if curl -s http://localhost:8081/q/health/ready > /dev/null 2>&1; then
        echo ""
        echo -e "${GREEN}✓${NC} Quarkus API is ready!"
        break
    fi
    sleep 3
    echo -n "."
done

echo ""

# ==============================================================================
# Final Status
# ==============================================================================

echo ""
echo -e "${GREEN}${BOLD}"
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                                                                ║"
echo "║          ✨  Platform Started Successfully!  ✨                ║"
echo "║                                                                ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo ""

echo -e "${CYAN}${BOLD}🌐 Access Points:${NC}"
echo -e "  ${GREEN}▸${NC} KartShoppe App:       ${GREEN}http://localhost:8081${NC}"
echo -e "  ${GREEN}▸${NC} Quarkus Dev UI:       ${GREEN}http://localhost:8081/q/dev${NC}"
echo -e "  ${GREEN}▸${NC} API Endpoints:        ${GREEN}http://localhost:8081/api/*${NC}"
echo -e "  ${GREEN}▸${NC} Kpow for Kafka:       ${GREEN}http://localhost:4000${NC}"
echo -e "  ${GREEN}▸${NC} Flex for Flink:       ${GREEN}http://localhost:5000${NC}"
echo -e "  ${GREEN}▸${NC} Flink Web UI:         ${GREEN}http://localhost:18081${NC}"
echo ""

echo -e "${CYAN}${BOLD}📊 Services Running:${NC}"
echo -e "  ${GREEN}✓${NC} PostgreSQL:           Port 5432"
echo -e "  ${GREEN}✓${NC} Redpanda (Kafka):     Port 19092"
echo -e "  ${GREEN}✓${NC} Quarkus + Frontend:   Port 8081"
echo -e "  ${GREEN}✓${NC} Kpow:                 Port 4000"
echo -e "  ${GREEN}✓${NC} Flex:                 Port 5000"
echo -e "  ${GREEN}✓${NC} Flink Web UI:         Port 18081"
echo ""

echo -e "${CYAN}${BOLD}🎓 Training Modules (Run Separately):${NC}"
echo -e "  ${YELLOW}▸${NC} Module 1 - Inventory + Orders: ${GREEN}./flink-inventory-with-orders-job.sh${NC}"
echo -e "  ${YELLOW}▸${NC} Module 2 - Order CDC:          ${GREEN}./flink-order-cdc-job.sh${NC}"
echo ""

echo -e "${BLUE}📁 Logs:${NC}"
echo -e "  Quarkus: ${CYAN}tail -f logs/quarkus.log${NC}"
echo -e "  Docker:  ${CYAN}docker compose logs -f${NC}"
echo ""

echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}To stop the platform:${NC} ${GREEN}./stop-platform-managed.sh${NC}"
echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Function to cleanup on exit
cleanup() {
    echo ""
    echo -e "${YELLOW}Shutting down platform...${NC}"
    kill $QUARKUS_PID 2>/dev/null || true
    docker compose -f compose-remote.yml down
    rm -rf .pids
    echo -e "${GREEN}Platform stopped${NC}"
    exit 0
}

# Set up trap to cleanup on Ctrl+C
trap cleanup INT

echo -e "${GREEN}Press Ctrl+C to stop the platform${NC}"
echo ""

# Keep the script running
wait
