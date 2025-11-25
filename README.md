# 🎓 APAC Roadshow 2026

## Overview

Welcome to the APAC Roadshow 2026 workshop! This guide provides complete instructions for setting up and running the KartShoppe training platform **from a blank slate**. KartShoppe is a fully-functional, real-time e-commerce application designed to teach modern data engineering principles.

This session is designed for participants to start with just the source code and progressively build, deploy, and enhance the platform by introducing powerful stream processing capabilities with Apache Kafka and Apache Flink.

---

## 🎯 Key Learning Objectives

By the end of this workshop, you will have hands-on experience with:

- **Event-Driven Architecture:** Understand how to use Kafka as the central nervous system of a modern, decoupled application.
- **Apache Flink Fundamentals:** Go from zero to building sophisticated, stateful stream processing jobs. You'll learn to implement sources, sinks, transformations, and windowing to solve real business problems.
- **Reactive Microservices:** See how a Quarkus-based backend can consume, process, and serve data from Flink and Kafka, pushing live updates directly to a web UI.
- **Database Change Data Capture (CDC):** Learn to capture row-level changes from a PostgreSQL database in real-time and turn them into an event stream for Flink to process.
- **Stateful Stream Processing:** Implement practical, stateful logic to solve classic e-commerce challenges like real-time inventory management.

---

## 💡 Training Philosophy

This setup follows a **progressive, hands-on learning approach** designed for maximum impact:

1.  **Start with a Working System:** You'll begin by launching the core KartShoppe application. It's a functional e-commerce site with a UI, API, and message broker, but with a key piece missing: **real-time data processing**.

2.  **Incremental Enhancements:** The workshop guides you through developing and deploying specific Flink jobs. You won't just learn theory; you'll solve a real business problem with each job you write.

3.  **Tangible, Visual Feedback:** As soon as you deploy a Flink job, you will see a new feature come to life in the KartShoppe UI. Watch as inventory counts update in real-time and order statuses change instantly based on user behavior. This immediate feedback loop makes learning concrete and rewarding.

---

## 🌟 Application Highlights

### Overall Architecture

![](./images/overview.png)

- **Frontend App**: The user interface within the browser that interacts with the backend via REST APIs and WebSockets.
- **Quarkus API**: The backend application that handles core business logic. It writes orders to the Postgres database and produces raw order events into the Kafka cluster. It also consumes processed _inventory_ events from Kafka to update the frontend.
- **Postgres**: The primary transactional database.
- **Kafka Cluster**: The central message bus for all real-time events.
- **Apache Flink Cluster**: A stream processing platform that runs applications for order processing and inventory management in real time.
- **Tooling (Kpow and Flex)**: Provides monitoring and management capabilities for the Kafka and Flink clusters, respectively.

### Quarkus API

![](./images/backend-api.png)

The Quarkus API serves as the central backend, handling synchronous user-facing interactions and asynchronous event processing. While it exposes multiple REST endpoints for actions like viewing products and managing the shopping cart, its most critical functions revolve around the checkout process and real-time data synchronization.

- **Checkout and Inventory Event Logic**: The `/checkout` endpoint orchestrates the core order processing flow. After persisting the order to the PostgreSQL database, it employs conditional logic based on a `useCdc` flag from the frontend:

  - **When `useCdc` is `false`**: The API is directly responsible for triggering inventory updates. It iterates through the order's items and uses the `orderEventEmitter` to produce an event for each item onto a Kafka topic. These specific events are consumed downstream by the Flink Inventory Job to calculate inventory changes in real time.
  - **When `useCdc` is `true`**: This direct event publishing is skipped. The system instead relies on the database transaction being captured by the Flink CDC Job to drive the inventory workflow.

- **Real-time Product State Management**: The API uses two distinct Kafka consumer technologies that work together to build a complete, real-time view of product data.

  - The **Kafka Streams application** consumes events from the `inventory-events` topic. It processes these partial updates (e.g., stock level changes) and merges them into the full product objects stored in the cache.
  - A standard **Kafka Consumer** (`ProductConsumer`) subscribes to the `products` topic, which contains full product details (e.g., name, description, initial price).

- **Unified Cache and WebSocket Updates**: Both the Kafka Streams app and the Kafka Consumer, upon receiving their respective events, call a central update function. This unified logic ensures data consistency by performing two actions simultaneously:
  1.  **Updates Internal Cache**: It modifies the in-memory state store managed by the `ProductCacheService`. This ensures that REST API calls always retrieve the most current product data with low latency.
  2.  **Pushes to Frontend**: It sends the complete, updated product object to all connected clients via the `EcommerceWebsocket`, ensuring the user interface reflects every change in real-time.

### Flink Applications

![](./images/flink-apps.png)

The stream processing layer of the architecture is composed of two distinct and composable Flink jobs that work in tandem to process order data and manage product inventory in real-time.

#### Order Processing and Inventory Management (`InventoryManagementJobWithOrders`)

This is the core stateful processing job that maintains a real-time view of product inventory. It is a sophisticated application that demonstrates several key Flink patterns.

- **Purpose**: To consume product catalog updates and real-time order events, calculate inventory levels, generate alerts, and publish enriched data streams for use by other services.
- **Key Patterns Implemented**:

  - **Pattern 01: Hybrid Source for State Bootstrapping**: The product data stream is created using a `HybridSource`. This powerful pattern first reads a file (`initial-products.json`) to bootstrap the job with the complete product catalog, and then seamlessly switches to reading from a Kafka topic for continuous, real-time updates.
  - **Pattern 02: Co-Processing Multiple Streams**: The job's core logic uses a `CoProcessFunction` to `connect` two distinct streams: the product stream (created by the Hybrid Source) and the order stream (from the `order-events` topic). This allows for unified logic and state management across different event types.
  - **Pattern 03: Shared Keyed State**: The `CoProcessFunction` maintains `ValueState` keyed by `productId`. This ensures that both product updates and order deductions modify the same, consistent state for any given product.
  - **Pattern 04: Timers for Event Generation**: The application uses Flink’s built-in timer mechanism to actively monitor the state of each product. The `CoProcessFunction` registers a processing time timer for each `productId` that is set to fire at a future point in time (e.g., one hour later). Crucially, whenever a new event arrives for that product—either a catalog update or an order deduction—the existing timer is cancelled and a new one is registered. This action effectively resets the "staleness" clock. If no events arrive for that product within the configured timeout period, the timer will fire, triggering the `onTimer` callback method. This callback then generates and emits a specific `STALE_INVENTORY` event, proactively signaling that the product's data might be out-of-date or require investigation.
  - **Pattern 05: Side Outputs**: To separate concerns, the job routes different types of business alerts (`LOW_STOCK`, `OUT_OF_STOCK`, `PRICE_DROP`) away from the main data flow into dedicated side outputs. These are later unioned and sent to a specific alerts topic.
  - **Pattern 06: Data Enrichment & Republishing**: The job acts as an enrichment service. It consumes raw product data, parses it into a clean format, and sinks a canonical version to the `products` topic. This provides a reliable, well-structured stream for other microservices.
  - **Output Streams**: The job produces several distinct output streams to different Kafka topics:
    - `products`: Enriched, clean product data for general consumption.
    - `inventory-events`: The main stream of events representing every change in inventory or price.
    - `inventory-alerts`: A dedicated stream for all generated alerts.
    - `websocket_fanout`: A copy of the inventory events, intended for real-time UI updates.

#### Flink CDC (`OrderCDCJob`)

This job acts as a real-time data pipeline that bridges the transactional database with the event-streaming ecosystem using Change Data Capture (CDC).

- **Purpose**: Its sole responsibility is to capture `INSERT` events from the `order_items` table in PostgreSQL as they happen.
- **Process Flow**:
  1.  **Capture**: It uses the Flink CDC Source, backed by Debezium, to non-intrusively read the PostgreSQL Write-Ahead Log (WAL).
  2.  **Filter**: The raw stream of database changes is immediately filtered to isolate only the creation of new rows (`op: "c"`) in the `order_items` table.
  3.  **Transform**: Each captured event is transformed by a `MapFunction` into a simple, clean JSON message containing only the essential fields: `productId`, `quantity`, `orderId`, and `timestamp`.
  4.  **Publish**: The clean JSON events are published to the `order-events` Kafka topic.

---

## 🚀 Before Training Day

### Prerequisites

Ensure students have:

- **Docker Desktop** installed and running
- **Node.js**: Version `18.x` or higher.
- **Python**: Version `3.10` or higher.
  - Note it must include `pip` (package installer) and `venv` (virtual environment support).
- **8GB RAM** minimum (16GB recommended)
- **Internet connection** (for downloading dependencies)
- **macOS or Linux** (Windows with WSL2 works too)

### One-Time Environment Setup

**Run this script ONCE before the training:**

```bash
./setup-environment.sh
```

This installs:

- ✅ SDKMAN (Java version manager)
- ✅ Java 11 (for building Flink jobs)
- ✅ Java 17 (for running Quarkus)
- ✅ Verifies Docker Desktop
- ✅ Verifies Node.js 18+
- ✅ Verifies Python 3.10+

**Estimated time:** 5-10 minutes

**Expected output:**

```
╔════════════════════════════════════════════════════════════════╗
║              ✨  Setup Completed Successfully!  ✨             ║
╚════════════════════════════════════════════════════════════════╝

Verified & Installed Components:
  ✓ Docker:     Docker version 24.x
  ✓ Node.js:    v20.x.x
  ✓ npm:        10.x.x
  ✓ Python:     Python 3.12.3
  ✓ SDKMAN:     5.x.x
  ✓ Java 11:    11.0.25-tem
  ✓ Java 17:    17.0.13-tem
```

---

## 📚 Training Day Schedule

### Setup Instaclustr Instances

For this workshop, we will set up the core data infrastructure for our application using Instaclustr's managed platform. Your tasks are separated into two parts: creating a Kafka cluster and connecting to a pre-provisioned Postgres database.

⏩ Skip this section if you are using local Kafka and PostgreSQL instances.

#### 1. Your Task: Create an Apache Kafka Cluster

Your first task is to provision a new Apache Kafka cluster. This will serve as the central message bus for all real-time events in our application.

- Please follow the official Instaclustr guide to create your cluster: **[Creating an Apache Kafka Cluster](https://www.instaclustr.com/support/documentation/apache-kafka/getting-started/creating-a-kafka-cluster/)**.

Once the cluster is provisioned and running, take note of your connection details (especially the Broker Addresses and credentials), as you will need them for the upcoming steps.

#### 2. Provided For You: PostgreSQL Database

To pre-configure logical replication for Flink CDC, a PostgreSQL database has already been provisioned for the workshop.

- You **do not** need to create this yourself.
- Your workshop instructor will provide you with the necessary connection details (host, port, database name, user, and password).

### Request Factor House Community License

In this workshop, we'll use Kpow and Flex to monitor Apache Kafka and Apache Flink clusters. Both tools are covered by a single, free Community license that you need to activate.

Please follow these two simple steps:

1.  **Generate Your License**: Go to the [Factor House Getting Started](https://account.factorhouse.io/auth/getting_started) page to generate your personal license.
2.  **Create `license.env` File**: Once generated, copy the license environment variables and paste them into a new file named `license.env`. You can use `license.env.example` as a reference for the correct format.

![](./images/license.png)

### Platform Startup

#### Goal

Get the core KartShoppe platform running **without any Flink jobs**.

#### What's Running?

```
┌─────────────────────────────────────────────────────────────┐
│  PLATFORM ARCHITECTURE (No Flink Yet)                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Frontend (React) ←─ Quinoa ─→ Quarkus API                  │
│                                     ↓                       │
│                                WebSockets                   │
│                                     ↓                       │
│                                   Kafka                     │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

<br>

Here are steps to start and stop the training platform depending on which instances (Instaclustr or local) are used for the workshop.

<details>
  <summary><b>🚀 Running with Instaclustr Managed Services</b></summary>

---

#### 1. Configure Connection Credentials

First, we need to tell our application how to connect to your newly created Kafka cluster and the provided PostgreSQL database.

- Open the `.env` file in the project root.
- Using the connection details from your Instaclustr Kafka cluster and the provided PostgreSQL credentials, fill in the required values.
- You can use the `.env.remote` file as a template to see which variables are needed.

---

#### 2. Activate the Remote Configuration for the Backend API

The Quarkus backend API needs a specific configuration file to connect to remote services. This command applies the configuration required for the application to work with Instaclustr services.

```bash
# This replaces the default properties with the one configured for remote connections
cp quarkus-api/src/main/resources/application.properties.remote \
   quarkus-api/src/main/resources/application.properties
```

---

#### 3. Create the Kafka Topics

Our Flink jobs need specific topics in Kafka to read from and write to. The following script will set up a Python virtual environment and create them for you.

```bash
# Create and activate a Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Install required Python packages
pip install -r scripts/requirements.txt

# Run the script to create topics on your Instaclustr Kafka cluster
python scripts/manage_topics.py --action create
```

---

#### 4. Initialize the Database Schema

Next, we'll run a script to create the `orders` and `order_items` tables in your provided PostgreSQL database.

```bash
# Ensure your virtual environment is still active
source venv/bin/activate

# Run the script to create the necessary tables
python scripts/manage_db.py --action init
```

---

#### 5. Launch the Core Platform & Applications

This step launches all the moving parts of our system. The `start-platform-remote.sh` script performs two key actions:

1.  **Starts Local Services in Docker**: It launches the monitoring tools (Kpow, Flex) and the Flink cluster (JobManager, TaskManager) as Docker containers.
2.  **Starts Applications Locally**: It runs the Quarkus backend API and the frontend application directly on your machine.

```bash
./start-platform-remote.sh
```

**✅ Verification:** Once everything is running, check the following URLs:

- **KartShoppe App**: [http://localhost:8081](http-:-//localhost:8081)
- **Kpow for Kafka**: [http://localhost:4000](http-:-//localhost:4000)
- **Flex for Flink**: [http://localhost:5000](http-:-//localhost:5000)

> **Note:** The product page in the KartShoppe UI will be empty. This is expected! Our Flink job, which is responsible for populating the product data, isn't running yet.

![](./images/prod-initial.png)

---

#### 6. Deploy the Inventory Management Flink Job

Now, let's deploy our main Flink job. This job reads the initial product data, calculates inventory, and publishes the results to Kafka, which the UI is listening to.

```bash
./flink-inventory-with-orders-job.sh
```

**✅ Verification:** After a few moments, refresh the KartShoppe UI. You should now see the products populated!

> **Try it out!** You can now experiment with adding items to your cart and completing a purchase (leave the "Use Flink CDC" box unchecked for now). Watch the inventory levels change in real time.

![](./images/prod-pop.png)

---

#### 7. Deploy the Flink CDC Job

Finally, deploy the second Flink job. This job uses Change Data Capture (CDC) to read order data directly from the database transaction log instead of from a direct Kafka event.

```bash
./flink-order-cdc-job.sh
```

**✅ Verification:** To test this new data path, add items to your cart and proceed to checkout.

> **Important:** On the checkout page, be sure to check the **Use Flink CDC** box before completing the purchase.

![](./images/prod-cdc.png)

#### 8. Stopping the Workshop Environment

When you are finished with your session, you can shut down all the local components by running:

```bash
./stop-platform-remote.sh
```

This script will stop the local Docker containers and terminate the Quarkus and frontend processes. It **will not** affect your remote Instaclustr services.

</details>

<br>

<details>
  <summary><b>🐋 Running with Local Instances</b></summary>

---

#### 1. Configure Connection Credentials

First, we need to tell our application how to connect to the Kafka cluster and PostgreSQL database running in Docker.

```bash
cp .env.local .env
```

---

#### 2. Activate the Local Configuration for the Backend API

The Quarkus backend API needs a specific configuration file to connect to the local instances. This command applies the configuration required for the application to work with the local instances.

```bash
# This replaces the default properties with the one configured for local connections
cp quarkus-api/src/main/resources/application.properties.local \
   quarkus-api/src/main/resources/application.properties
```

---

#### 3. Launch the Core Platform

This step launches all the moving parts of our system. The `start-platform-local.sh` script performs two key actions:

1.  **Starts Local Infrastructure in Docker**: It launches the entire local data platform as Docker containers. This includes:
    - A Kafka cluster (using Redpanda)
    - A PostgreSQL database
    - Monitoring tools (Kpow, Flex)
    - A Flink cluster (JobManager, TaskManager)
2.  **Starts Applications Locally**: It runs the Quarkus backend API and the frontend application directly on your machine.

```bash
./start-platform-local.sh
```

**✅ Verification:** Once everything is running, check the following URLs:

- **KartShoppe App**: [http://localhost:8081](http-:-//localhost:8081)
- **Kpow for Kafka**: [http://localhost:4000](http-:-//localhost:4000)
- **Flex for Flink**: [http://localhost:5000](http-:-//localhost:5000)

> **Note:** The product page in the KartShoppe UI will be empty. This is expected! Our Flink job, which is responsible for populating the product data, isn't running yet.

![](./images/prod-initial.png)

---

#### 4. Deploy the Inventory Management Flink Job

Now, let's deploy our main Flink job. This job reads the initial product data, calculates inventory, and publishes the results to Kafka, which the UI is listening to.

```bash
./flink-inventory-with-orders-job.sh
```

**✅ Verification:** After a few moments, refresh the KartShoppe UI. You should now see the products populated!

> **Try it out!** You can now experiment with adding items to your cart and completing a purchase (leave the "Use Flink CDC" box unchecked for now). Watch the inventory levels change in real time.

![](./images/prod-pop.png)

---

#### 5. Deploy the Flink CDC Job

Finally, deploy the second Flink job. This job uses Change Data Capture (CDC) to read order data directly from the database transaction log instead of from a direct Kafka event.

```bash
./flink-order-cdc-job.sh
```

**✅ Verification:** To test this new data path, add items to your cart and proceed to checkout.

> **Important:** On the checkout page, be sure to check the **Use Flink CDC** box before completing the purchase.

![](./images/prod-cdc.png)

#### 6. Stopping the Workshop Environment

When you are finished with your session, you can shut down all the local components by running:

```bash
./stop-platform-local.sh
```

This script will stop the local Docker containers and terminate the Quarkus and frontend processes.

</details>

---

#### Hands-On Exercise

1. Add 10 items of the same product to your cart
2. Watch the inventory count decrease in real-time
3. Check Flink dashboard to see events processed
4. View Kafka topic to see inventory update messages

---

## 🎓 Training Tips

### For Instructors

1. **Show, then explain:** Start each Flink job, show the visible change, THEN explain the concepts
2. **Use the UI constantly:** Keep the browser open, refresh often to show real-time updates
3. **Monitor Flink dashboard:** Show the job graph, metrics, backpressure (http://localhost:8081)
4. **Explore Kafka topics:** Use Redpanda Console to see messages flowing (http://localhost:4000)
5. **Explain incrementally:** Each module builds on the previous - don't overwhelm with all patterns at once

### For Students

1. **Follow along:** Run commands in your own terminal
2. **Experiment:** Try breaking things! Stop a Flink job, see what happens, restart it
3. **Ask questions:** Why does this pattern need keyed state? Why use broadcast state here?
4. **Monitor everything:** Open all dashboards (Flink, Redpanda, Quarkus Dev UI)
5. **Read the logs:** `tail -f logs/quarkus.log` and Flink job logs show what's happening

---

### Key URLs

| Service                   | URL                         |
| ------------------------- | --------------------------- |
| **KartShoppe App**        | http://localhost:8081       |
| **Quarkus Dev UI**        | http://localhost:8081/q/dev |
| **Kpow for Apache Kafka** | http://localhost:4000       |
| **Flex for Apache Flink** | http://localhost:5000       |

---

## 🐛 Troubleshooting

### Platform Won't Start

```bash
# Check Docker
docker info

# Check Java version
java -version  # Should be 17+
# if not
sdk use java 17.0.13-tem

# View logs
tail -f logs/quarkus.log
docker compose logs -f
```

### Flink Job Fails to Start

```bash
# Switch to Java 11 for building Flink jobs
sdk use java 11.0.25-tem

# Rebuild
./gradlew :flink-inventory:clean :flink-inventory:shadowJar

# Check logs
tail -f logs/inventory.log
```

### Port Conflicts

```bash
# Kill process on port 8081 (Quarkus)
lsof -ti:8081 | xargs kill -9

# Kill process on port 8082 (Flink, if running)
lsof -ti:8082 | xargs kill -9
```

### Clean Slate Reset

```bash
## Stop everything
# remote
./stop-platform-remote.sh
# local
./stop-platform-local.sh

## Clean Docker volumes
# remote
docker compose -f compose-remote.yml down -v
# local
docker compose -f compose-local.yml down -v

## Clean Gradle cache
./gradlew clean
# or
./clean.sh

## Restart
# remote
./start-platform-remote.sh
# local
./start-platform-local.sh
```

---

## 🎯 Learning Outcomes

By the end of this training, students will:

- ✅ Understand **Flink's core concepts**: streams, state, time, windows
- ✅ Build **stateful stream processing** applications
- ✅ Integrate **Kafka/Redpanda** for event streaming
- ✅ Combine **batch and streaming** with hybrid sources
- ✅ Build **reactive microservices** with Quarkus
- ✅ Create **real-time data pipelines** end-to-end

---

## 📖 Additional Resources

- [Apache Flink Documentation](https://flink.apache.org)
- [Quarkus Guides](https://quarkus.io/guides/)
- [Redpanda Documentation](https://docs.redpanda.com)
- [Ververica Academy](https://www.ververica.com/academy)
- [Factor House Docs](https://docs.factorhouse.io/)
- [Instaclustr Documentation](https://www.instaclustr.com/support/documentation/)
