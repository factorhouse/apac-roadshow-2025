package com.ververica.composable_job.flink.inventory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.composable_job.flink.inventory.patterns.hybrid_source.HybridSourceExample;
import com.ververica.composable_job.flink.inventory.shared.config.InventoryConfig;
import com.ververica.composable_job.flink.inventory.shared.model.AlertEvent;
import com.ververica.composable_job.flink.inventory.shared.model.InventoryEvent;
import com.ververica.composable_job.flink.inventory.shared.model.OrderItemDeduction;
import com.ververica.composable_job.flink.inventory.shared.processor.*;
import com.ververica.composable_job.model.ecommerce.Product;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inventory Management Job WITH ORDER DEDUCTION - Advanced Pattern Composition
 *
 * This enhanced version processes both product updates and customer orders to maintain
 * a real-time inventory count, and now publishes a clean 'products' topic.
 *
 * PATTERNS DEMONSTRATED:
 *
 * 1. PATTERN 01: Multiple Sources & Co-Processing
 *    - The job `connect`s two distinct data streams into a single `CoProcessFunction`:
 *      a) The Product Stream, which uses a `Hybrid Source` to bootstrap from a file
 *         (`data/initial-products.json`) before switching to an unbounded Kafka topic.
 *      b) The Order Stream, which consumes real-time order events from Kafka.
 *
 * 2. PATTERN 02: Shared Keyed State
 *    - The `CoProcessFunction` uses `ValueState` to maintain the inventory and price for
 *      each product key. This allows both the product and order streams to read from and
 *      write to the same state for a given product.
 *
 * 3. PATTERN 03: Timers
 *    - The `CoProcessFunction` uses processing time timers to detect and flag products
 *      with stale inventory (no updates for a set period).
 *
 * 4. PATTERN 04: Side Outputs
 *    - Alerts for `LOW_STOCK`, `OUT_OF_STOCK`, and `PRICE_DROP` are routed from the
 *      main process into dedicated streams for separate downstream handling.
 *
 * 5. PATTERN 05: Data Enrichment & Republishing
 *    - The job consumes raw product data, parses it into a clean `Product` object,
 *      and sinks it to a canonical `products` topic for other microservices to use.
 *
 * ARCHITECTURE (Updated):
 * <pre>
 * Product File  ──┐
 *                 ├─→ Hybrid Source → Product Parser ──┬── ▶ Kafka: products (for other services)
 * Product Kafka ──┘                                    │
 *                                                      ▼
 *                                         CoProcessFunction (Shared Keyed State)
 *                                                      ▲
 * Order Kafka ───→ Order Parser ───────────────────────┘
 *                                         │
 *                   ┌─────────────────────┼─────────────────────┐
 *                   ▼                     ▼                     ▼
 *             Inventory Events    Low Stock Alerts    Out of Stock Alerts
 *                   │                     │                     │
 *                   ▼                     └─────────────────────┘
 *           Kafka: inventory-events         Kafka: inventory-alerts
 *                   │
 *                   ▼
 *           Kafka: websocket_fanout (Real-time UI updates)
 * </pre>
 *
 * LEARNING OUTCOMES:
 * - Understand how to use a CoProcessFunction to manage shared state between two streams.
 * - See a full event-driven e-commerce architecture in action.
 * - Learn how a Flink job can act as a data enricher, consuming raw data and
 *   producing a clean, canonical topic for a broader microservices ecosystem.
 *
 * RUN THIS JOB:
 * <pre>
 * # 1. Start infrastructure
 * docker compose up -d
 *
 * # 2. Setup Kafka topics
 * ./0-setup-topics.sh
 *
 * # 3. Run this job
 * ./flink-1b-inventory-with-orders-job.sh
 *
 * # 4. Place orders in the UI
 * # Watch inventory decrease in real-time!
 * </pre>
 */
public class InventoryManagementJobWithOrders {

    private static final Logger LOG = LoggerFactory.getLogger(InventoryManagementJobWithOrders.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // ========================================
        // STEP 1: Setup Environment & Configuration
        // ========================================

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        InventoryConfig config = InventoryConfig.fromEnvironment();

        env.setParallelism(config.getParallelism());
        env.enableCheckpointing(config.getCheckpointInterval());

        LOG.info("🚀 Starting Inventory Management Job WITH ORDER DEDUCTION");
        LOG.info("📊 Parallelism: {}", config.getParallelism());
        LOG.info("💾 Checkpoint interval: {}ms", config.getCheckpointInterval());
        LOG.info("🆕 NEW: This job processes orders and deducts inventory!");

        // ========================================
        // STEP 2: PATTERN 01 (Part 1) - Product Source
        // ========================================

        LOG.info("\n📥 PATTERN 01: Creating Product Hybrid Source (File → Kafka)");

        HybridSource<String> productSource = HybridSourceExample.createHybridSource(config);

        DataStream<String> rawProductStream = env.fromSource(
            productSource,
            WatermarkStrategy.noWatermarks(),
            "Product Hybrid Source",
            Types.STRING
        );

        DataStream<Product> productStream = rawProductStream
            .process(new ProductParser())
            .name("Parse Product JSON")
            .uid("product-parser");

        // =============================================================
        // STEP 3: PATTERN 05 - Data Enrichment & Republishing
        // =============================================================

        LOG.info("\n📤 PATTERN 05: Sinking clean product data to 'products' topic for other services");

        KafkaSink<String> productsSink = KafkaSink.<String>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setKafkaProducerConfig(config.getKafkaPropertiesAsProperties())
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("products")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .build();

        productStream
            .map(product -> MAPPER.writeValueAsString(product))
            .sinkTo(productsSink)
            .name("Sink to products topic")
            .uid("products-sink");

        // ========================================
        // STEP 4: PATTERN 01 (Part 2) - Order Events Source
        // ========================================

        LOG.info("\n📦 PATTERN 01: Creating Order Events Source (Kafka)");
        LOG.info("   Topic: order-events");
        LOG.info("   Purpose: Real-time inventory deduction from orders");

        KafkaSource<String> orderEventsSource = KafkaSource.<String>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics("order-events")
            .setGroupId(config.getKafkaGroupId())
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawOrderStream = env.fromSource(
            orderEventsSource,
            WatermarkStrategy.noWatermarks(),
            "Order Events Source"
        );

        DataStream<OrderItemDeduction> orderStream = rawOrderStream
            .process(new OrderItemParser())
            .name("Parse Order Item JSON")
            .uid("order-parser");

        // =================================================================
        // STEP 5: PATTERNS 01, 02, 03, 04 - Co-Process and Shared State
        // =================================================================
        LOG.info("\n🤝 Connecting product and order streams to a CoProcessFunction...");
        LOG.info("   PATTERN 02: It will use Shared Keyed State for inventory.");
        LOG.info("   PATTERN 03: It will use Timers to detect stale products.");
        LOG.info("   PATTERN 04: It will use Side Outputs to route alerts.");

        SingleOutputStreamOperator<InventoryEvent> allInventoryEvents = productStream
                .keyBy(product -> product.productId)
                .connect(orderStream.keyBy(order -> order.productId))
                .process(new SharedInventoryProcessor())
                .name("Shared Inventory State Processor")
                .uid("shared-inventory-state");

        // Extract the side outputs for alerts directly from the new, single operator.
        DataStream<AlertEvent> allAlerts = allInventoryEvents.getSideOutput(SharedInventoryProcessor.LOW_STOCK_TAG)
                .union(allInventoryEvents.getSideOutput(SharedInventoryProcessor.OUT_OF_STOCK_TAG),
                    allInventoryEvents.getSideOutput(SharedInventoryProcessor.PRICE_DROP_TAG));

        // ========================================
        // STEP 6: Sinks - Output to Kafka
        // ========================================

        LOG.info("\n📤 Configuring Kafka Sinks");

        // All inventory events (product updates + order deductions)
        SinkFactory.sinkInventoryEvents(allInventoryEvents, config);

        // Alerts
        SinkFactory.sinkAlerts(allAlerts, config);

        // WebSocket for real-time UI updates
        SinkFactory.sinkToWebSocket(allInventoryEvents, config);

        // ========================================
        // STEP 7: Execute Job
        // ========================================

        LOG.info("\n✅ Job configured with COMPLETE inventory depletion!");
        LOG.info("🎯 Pattern Summary:");
        LOG.info("   01. Hybrid Source: ✓ (File → Kafka products)");
        LOG.info("   02. Keyed State: ✓ (Shared inventory state)");
        LOG.info("   03. Timers: ✓ (Stale detection)");
        LOG.info("   04. Side Outputs: ✓ (Alert routing)");
        LOG.info("   05. Multiple Sources: ✓ (Products + Orders)");
        LOG.info("   06. Order Deduction: ✓ (Real-time inventory)");
        LOG.info("\n💡 TRY IT:");
        LOG.info("   1. Place an order in the UI");
        LOG.info("   2. Watch inventory decrease in real-time!");
        LOG.info("   3. See out-of-stock alerts when inventory = 0");
        LOG.info("\n🚀 Executing job...\n");

        env.execute("Inventory Management Job WITH ORDER DEDUCTION");
    }
}
