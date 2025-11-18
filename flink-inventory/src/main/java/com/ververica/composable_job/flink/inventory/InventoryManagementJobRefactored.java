package com.ververica.composable_job.flink.inventory;

import com.ververica.composable_job.flink.inventory.patterns.hybrid_source.HybridSourceExample;
import com.ververica.composable_job.flink.inventory.shared.config.InventoryConfig;
import com.ververica.composable_job.flink.inventory.shared.model.AlertEvent;
import com.ververica.composable_job.flink.inventory.shared.model.InventoryEvent;
import com.ververica.composable_job.flink.inventory.shared.processor.InventoryStateFunction;
import com.ververica.composable_job.flink.inventory.shared.processor.ProductParser;
import com.ververica.composable_job.flink.inventory.shared.processor.SinkFactory;
import com.ververica.composable_job.model.ecommerce.Product;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inventory Management Job - Pattern Composition Example
 *
 * This refactored version demonstrates how to combine multiple Flink patterns
 * into a production-ready job. Each pattern is learned separately in the
 * patterns/ directory, then composed here.
 *
 * PATTERNS DEMONSTRATED:
 *
 * 1. PATTERN 01: Hybrid Source (Bounded → Unbounded)
 *    - Bootstrap from file: data/initial-products.json
 *    - Then stream from Kafka: product-updates topic
 *    - See: patterns/01_hybrid_source/
 *
 * 2. PATTERN 02: Keyed State
 *    - Track inventory per product using ValueState
 *    - Detect changes (stock up, stock down, price changes)
 *    - See: patterns/02_keyed_state/
 *
 * 3. PATTERN 03: Timers
 *    - Detect stale inventory (no updates for 1 hour)
 *    - Processing time timers for each product
 *    - See: patterns/03_timers/
 *
 * 4. PATTERN 04: Side Outputs
 *    - Route alerts to different streams
 *    - LOW_STOCK, OUT_OF_STOCK, PRICE_DROP alerts
 *    - See: patterns/04_side_outputs/
 *
 * ARCHITECTURE:
 * <pre>
 * File Source ──┐
 *               ├─→ Hybrid Source → Parse JSON → Key by productId
 * Kafka Source ─┘                                       │
 *                                                        ▼
 *                                            Inventory State Function
 *                                     (Patterns 02 + 03 + 04 combined)
 *                                                        │
 *                    ┌───────────────┬──────────────────┼────────────────┐
 *                    ▼               ▼                  ▼                ▼
 *              Main Events    Low Stock Alerts  Out of Stock    Price Drop Alerts
 *                    │               │                  │                │
 *                    ▼               └──────────────────┴────────────────┘
 *            Kafka: inventory-events          Kafka: inventory-alerts
 *                    │
 *                    ▼
 *            Kafka: websocket_fanout (for UI)
 * </pre>
 *
 * LEARNING PATH:
 * 1. Study patterns individually (patterns/01_* through patterns/04_*)
 * 2. Understand shared components (shared/model, shared/config, shared/processor)
 * 3. See how they compose in this main job
 * 4. Run and observe the complete pipeline
 *
 * RUN THIS JOB:
 * <pre>
 * # Start Kafka
 * docker compose up -d redpanda
 *
 * # Run the job
 * ./gradlew :flink-inventory:run
 *
 * # Observe logs
 * tail -f logs/inventory.log
 * </pre>
 */
public class InventoryManagementJobRefactored {

    private static final Logger LOG = LoggerFactory.getLogger(InventoryManagementJobRefactored.class);

    public static void main(String[] args) throws Exception {
        // ========================================
        // STEP 1: Setup Environment & Configuration
        // ========================================

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Load configuration from environment variables
        InventoryConfig config = InventoryConfig.fromEnvironment();

        // Apply configuration
        env.setParallelism(config.getParallelism());
        env.enableCheckpointing(config.getCheckpointInterval());

        LOG.info("🚀 Starting Inventory Management Job");
        LOG.info("📊 Parallelism: {}", config.getParallelism());
        LOG.info("💾 Checkpoint interval: {}ms", config.getCheckpointInterval());

        // ========================================
        // STEP 2: PATTERN 01 - Hybrid Source
        // ========================================

        LOG.info("\n📥 PATTERN 01: Creating Hybrid Source (File → Kafka)");
        LOG.info("   File: {}", config.getInitialProductsFile());
        LOG.info("   Kafka: {}", config.getKafkaBootstrapServers());

        // Use pattern example's factory method
        HybridSource<String> productSource = HybridSourceExample.createHybridSource(config);

        DataStream<String> rawProductStream = env.fromSource(
            productSource,
            WatermarkStrategy.noWatermarks(),
            "Product Hybrid Source (File + Kafka)",
            Types.STRING
        );

        // ========================================
        // STEP 3: Parse JSON → Product Objects
        // ========================================

        LOG.info("\n🔄 Parsing JSON to Product objects");

        DataStream<Product> productStream = rawProductStream
            .process(new ProductParser())
            .name("Parse JSON to Product")
            .uid("product-parser");

        // ========================================
        // STEP 4: PATTERNS 02, 03, 04 Combined
        // ========================================

        LOG.info("\n🔧 PATTERNS 02, 03, 04: Keyed State + Timers + Side Outputs");
        LOG.info("   - Keyed State: Track inventory per product");
        LOG.info("   - Timers: Detect stale inventory (1 hour timeout)");
        LOG.info("   - Side Outputs: Route alerts by type");

        // Key by product ID and apply stateful processing
        SingleOutputStreamOperator<InventoryEvent> inventoryEvents = productStream
            .keyBy(product -> product.productId)
            .process(new InventoryStateFunction())
            .name("Inventory State Processing (Patterns 02+03+04)")
            .uid("inventory-state-processor");

        // ========================================
        // STEP 5: PATTERN 04 - Extract Side Outputs
        // ========================================

        LOG.info("\n🎯 PATTERN 04: Extracting Side Output Streams");

        DataStream<AlertEvent> lowStockAlerts = inventoryEvents
            .getSideOutput(InventoryStateFunction.LOW_STOCK_TAG);

        DataStream<AlertEvent> outOfStockAlerts = inventoryEvents
            .getSideOutput(InventoryStateFunction.OUT_OF_STOCK_TAG);

        DataStream<AlertEvent> priceDropAlerts = inventoryEvents
            .getSideOutput(InventoryStateFunction.PRICE_DROP_TAG);

        // Union all alerts into single stream
        DataStream<AlertEvent> allAlerts = lowStockAlerts
            .union(outOfStockAlerts, priceDropAlerts);

        // ========================================
        // STEP 6: Sinks - Output to Kafka
        // ========================================

        LOG.info("\n📤 Configuring Kafka Sinks");

        // Main inventory events → inventory-events topic
        SinkFactory.sinkInventoryEvents(inventoryEvents, config);

        // All alerts → inventory-alerts topic
        SinkFactory.sinkAlerts(allAlerts, config);

        // WebSocket fanout for real-time UI updates
        SinkFactory.sinkToWebSocket(inventoryEvents, config);

        // ========================================
        // STEP 7: Execute Job
        // ========================================

        LOG.info("\n✅ All patterns configured successfully!");
        LOG.info("🎯 Pattern Summary:");
        LOG.info("   01. Hybrid Source: ✓ (File → Kafka)");
        LOG.info("   02. Keyed State: ✓ (Per-product tracking)");
        LOG.info("   03. Timers: ✓ (Stale detection)");
        LOG.info("   04. Side Outputs: ✓ (Alert routing)");
        LOG.info("\n🚀 Executing job...\n");

        env.execute("Inventory Management Job (Pattern Composition)");
    }
}
