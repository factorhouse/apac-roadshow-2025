#!/bin/bash
# Start the ENHANCED Inventory Management Flink Job WITH ORDER DEDUCTION
#
# This version includes:
# - Product inventory tracking (from file + Kafka)
# - Order-based inventory deduction (from order-events topic)
# - Real-time inventory updates when orders are placed

set -e

echo "🚀 Starting ENHANCED Inventory Management Job (WITH ORDER DEDUCTION)..."

# Load and Print Environment Configuration from .env file
source env.sh
load_and_print_env

echo ""
echo "🆕 This job includes ORDER DEDUCTION:"
echo "   - Listens to order-events topic"
echo "   - Deducts inventory when orders are placed"
echo "   - Shows complete e-commerce inventory flow"
echo ""

# Build the job if needed
echo ""
echo "📦 Building Enhanced Inventory Flink Job..."
./gradlew :flink-inventory:shadowJar

##
## Deploy the Flink job to a local standalone Flink cluster
##
# Copy the Jar file and initial product file
echo ""
echo "📋 Copying ENHANCED Inventory Management Job Jar..."
docker cp flink-inventory/build/libs/flink-inventory.jar \
  jobmanager:/tmp/flink-inventory.jar
echo "📋 Copying the initial product file..."
docker cp data/initial-products.json jobmanager:/tmp/initial-products.json \
  && docker cp data/initial-products.json taskmanager:/tmp/initial-products.json

# Deploy the Job
echo ""
echo "▶️  Deploying ENHANCED Inventory Management Job..."
echo ""
docker exec jobmanager /opt/flink/bin/flink run -d \
  -Denv.java.opts="--add-opens java.base/java.util=ALL-UNNAMED" \
  -c com.ververica.composable_job.flink.inventory.InventoryManagementJobWithOrders \
  /tmp/flink-inventory.jar

echo ""
echo "✅ Enhanced Inventory Management Job deployed"

##
## Alternative way to run the Flink job using a MiniCluster
##
# # Get the script directory
# SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# ...

# echo ""
# java --add-opens java.base/java.util=ALL-UNNAMED \
#   -cp flink-inventory/build/libs/flink-inventory.jar \
#   com.ververica.composable_job.flink.inventory.InventoryManagementJobWithOrders

# echo ""
# echo "✅ Enhanced Inventory Management Job completed"
