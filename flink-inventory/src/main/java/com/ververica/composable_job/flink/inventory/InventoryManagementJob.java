package com.ververica.composable_job.flink.inventory;

import com.ververica.composable_job.model.ecommerce.Product;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Inventory Management Job using Hybrid Source
 * 
 * This job demonstrates Flink 1.20's Hybrid Source capability:
 * 1. First reads initial product catalog from a file (bounded source)
 * 2. Then seamlessly switches to reading updates from Kafka (unbounded source)
 * 
 * This pattern is perfect for bootstrapping state from historical data
 * before processing real-time events.
 */
public class InventoryManagementJob {
    private static final Logger LOG = LoggerFactory.getLogger(InventoryManagementJob.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    public static void main(String[] args) throws Exception {
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // Set parallelism for better performance
        
        // Configuration
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092");
        String initialProductsFile = System.getenv().getOrDefault("INITIAL_PRODUCTS_FILE", "data/initial-products.json");
        
        LOG.info("Starting Inventory Management Job with Hybrid Source");
        LOG.info("Initial products file: {}", initialProductsFile);
        LOG.info("Kafka bootstrap servers: {}", bootstrapServers);
        
        // Create Hybrid Source
        HybridSource<String> hybridSource = createHybridSource(initialProductsFile, bootstrapServers);
        
        // Process data from hybrid source with explicit type information
        DataStream<String> rawProducts = env.fromSource(
            hybridSource,
            WatermarkStrategy.<String>noWatermarks(),
            "Hybrid Source: File + Kafka",
            Types.STRING
        );
        
        // Parse JSON to Product objects
        DataStream<Product> products = rawProducts
            .process(new JsonToProductParser())
            .name("Parse JSON to Product")
            .uid("json-parser");
        
        // Send products directly to the products topic for the ProductCacheService
        KafkaSink<String> productsSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("products")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .build();
        
        products
            .map(product -> MAPPER.writeValueAsString(product))
            .sinkTo(productsSink)
            .name("Sink to products topic")
            .uid("products-sink");
        
        // Key by product ID and process inventory changes
        DataStream<InventoryEvent> inventoryEvents = products
            .keyBy(product -> product.productId)
            .process(new InventoryStateProcessor())
            .name("Process Inventory State")
            .uid("inventory-processor");
        
        // Create sinks
        setupKafkaSinks(inventoryEvents, bootstrapServers);
        
        // Execute job
        env.execute("Inventory Management with Hybrid Source");
    }
    
    /**
     * Creates a Hybrid Source that:
     * 1. First reads from a file (bounded)
     * 2. Then switches to Kafka (unbounded)
     */
    private static HybridSource<String> createHybridSource(String filePath, String bootstrapServers) {
        // Create bounded file source for initial load - using custom format to read entire file
        FileSource<String> fileSource = FileSource
            .forRecordStreamFormat(new WholeFileStreamFormat(), new Path(filePath))
            .build();
        
        // Create unbounded Kafka source for continuous updates
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setTopics("product-updates")
            .setGroupId("inventory-management-hybrid")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperty("partition.discovery.interval.ms", "10000") // Discover new partitions
            .setProperty("commit.offsets.on.checkpoint", "true")
            .build();
        
        // Build hybrid source: file -> kafka
        HybridSource<String> hybridSource = HybridSource.builder(fileSource)
            .addSource(kafkaSource)
            .build();
        
        LOG.info("Created Hybrid Source: File -> Kafka");
        return hybridSource;
    }
    
    /**
     * Process function to parse JSON strings to Product objects
     * Handles both single products and arrays of products
     */
    public static class JsonToProductParser extends ProcessFunction<String, Product> {
        private static final Logger LOG = LoggerFactory.getLogger(JsonToProductParser.class);
        private static final ObjectMapper mapper = new ObjectMapper();
        
        @Override
        public void processElement(String value, Context ctx, Collector<Product> out) throws Exception {
            if (value == null || value.trim().isEmpty()) {
                return;
            }
            
            try {
                String trimmedValue = value.trim();
                
                // Check if this is a JSON array
                if (trimmedValue.startsWith("[")) {
                    // Parse as array of products
                    Product[] products = mapper.readValue(trimmedValue, Product[].class);
                    for (Product product : products) {
                        LOG.info("Successfully parsed product from array: {}", product.productId);
                        out.collect(product);
                    }
                } else if (trimmedValue.startsWith("{")) {
                    // Parse as single product
                    Product product = mapper.readValue(trimmedValue, Product.class);
                    LOG.info("Successfully parsed single product: {}", product.productId);
                    out.collect(product);
                } else {
                    LOG.warn("Received non-JSON data: {}", trimmedValue.substring(0, Math.min(100, trimmedValue.length())));
                }
            } catch (Exception e) {
                LOG.error("Failed to parse JSON: {}", e.getMessage());
                LOG.debug("JSON content: {}", value.substring(0, Math.min(200, value.length())));
            }
        }
    }
    
    /**
     * Stateful processor that tracks inventory changes and generates events
     */
    public static class InventoryStateProcessor extends KeyedProcessFunction<String, Product, InventoryEvent> {
        private ValueState<Product> lastProductState;
        private ValueState<Long> lastUpdateTime;
        
        @Override
        public void open(Configuration parameters) {
            // Initialize state
            ValueStateDescriptor<Product> productDescriptor = new ValueStateDescriptor<>(
                "last-product-state",
                TypeInformation.of(Product.class)
            );
            lastProductState = getRuntimeContext().getState(productDescriptor);
            
            ValueStateDescriptor<Long> timeDescriptor = new ValueStateDescriptor<>(
                "last-update-time",
                TypeInformation.of(Long.class)
            );
            lastUpdateTime = getRuntimeContext().getState(timeDescriptor);
        }
        
        @Override
        public void processElement(Product product, Context ctx, Collector<InventoryEvent> out) throws Exception {
            Product previousProduct = lastProductState.value();
            Long previousUpdateTime = lastUpdateTime.value();
            
            long currentTime = System.currentTimeMillis();
            InventoryEvent event = new InventoryEvent();
            event.productId = product.productId;
            event.productName = product.name;
            event.category = product.category;
            event.timestamp = currentTime;
            event.currentProduct = product;
            
            if (previousProduct == null) {
                // First time seeing this product
                event.eventType = EventType.PRODUCT_ADDED;
                event.previousInventory = 0;
                event.currentInventory = product.inventory;
                event.previousPrice = 0.0;
                event.currentPrice = product.price;
                event.changeReason = "New product added to catalog";
                event.isInitialLoad = true;
                
                LOG.info("New product added: {} with inventory: {}", product.productId, product.inventory);
            } else {
                // Product update
                event.previousInventory = previousProduct.inventory;
                event.currentInventory = product.inventory;
                event.previousPrice = previousProduct.price;
                event.currentPrice = product.price;
                event.isInitialLoad = false;
                
                // Calculate time since last update
                long timeSinceLastUpdate = previousUpdateTime != null ? 
                    currentTime - previousUpdateTime : 0;
                event.millisSinceLastUpdate = timeSinceLastUpdate;
                
                // Determine event type
                if (previousProduct.inventory != product.inventory) {
                    if (product.inventory == 0) {
                        event.eventType = EventType.OUT_OF_STOCK;
                        event.changeReason = "Product is now out of stock";
                    } else if (previousProduct.inventory == 0 && product.inventory > 0) {
                        event.eventType = EventType.BACK_IN_STOCK;
                        event.changeReason = String.format("Product restocked with %d units", product.inventory);
                    } else if (product.inventory < previousProduct.inventory) {
                        event.eventType = EventType.INVENTORY_DECREASED;
                        int sold = previousProduct.inventory - product.inventory;
                        event.changeReason = String.format("%d units sold/removed", sold);
                    } else {
                        event.eventType = EventType.INVENTORY_INCREASED;
                        int added = product.inventory - previousProduct.inventory;
                        event.changeReason = String.format("%d units added to inventory", added);
                    }
                } else if (Math.abs(previousProduct.price - product.price) > 0.01) {
                    event.eventType = EventType.PRICE_CHANGED;
                    double priceDiff = product.price - previousProduct.price;
                    event.changeReason = String.format("Price %s by $%.2f", 
                        priceDiff > 0 ? "increased" : "decreased", Math.abs(priceDiff));
                } else {
                    event.eventType = EventType.PRODUCT_UPDATED;
                    event.changeReason = "Product metadata updated";
                }
                
                LOG.info("Product {} updated: {} ({}ms since last update)", 
                    product.productId, event.eventType, timeSinceLastUpdate);
            }
            
            // Update state
            lastProductState.update(product);
            lastUpdateTime.update(currentTime);
            
            // Emit the main event
            out.collect(event);
            
            // Generate additional alerts
            generateAlerts(product, event, out);
            
            // Set timer for inventory check (e.g., alert if no update for 1 hour)
            ctx.timerService().registerProcessingTimeTimer(currentTime + 3600000); // 1 hour
        }
        
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<InventoryEvent> out) throws Exception {
            Product product = lastProductState.value();
            Long lastUpdate = lastUpdateTime.value();
            
            if (product != null && lastUpdate != null) {
                long timeSinceUpdate = timestamp - lastUpdate;
                if (timeSinceUpdate > 3600000) { // More than 1 hour
                    InventoryEvent staleAlert = new InventoryEvent();
                    staleAlert.productId = product.productId;
                    staleAlert.productName = product.name;
                    staleAlert.eventType = EventType.STALE_INVENTORY;
                    staleAlert.changeReason = String.format("No updates for %.1f hours", timeSinceUpdate / 3600000.0);
                    staleAlert.timestamp = timestamp;
                    staleAlert.currentInventory = product.inventory;
                    out.collect(staleAlert);
                }
            }
        }
        
        private void generateAlerts(Product product, InventoryEvent mainEvent, Collector<InventoryEvent> out) {
            // Low stock alert
            if (product.inventory > 0 && product.inventory <= 10) {
                InventoryEvent alert = new InventoryEvent();
                alert.productId = product.productId;
                alert.productName = product.name;
                alert.category = product.category;
                alert.eventType = EventType.LOW_STOCK_ALERT;
                alert.currentInventory = product.inventory;
                alert.changeReason = String.format("Low stock warning: only %d items remaining", product.inventory);
                alert.timestamp = System.currentTimeMillis();
                alert.alertLevel = product.inventory <= 5 ? "CRITICAL" : "WARNING";
                out.collect(alert);
            }
            
            // Price drop alert for sales
            if (mainEvent.eventType == EventType.PRICE_CHANGED && 
                mainEvent.currentPrice < mainEvent.previousPrice) {
                double discount = (mainEvent.previousPrice - mainEvent.currentPrice) / mainEvent.previousPrice * 100;
                if (discount >= 10) {
                    InventoryEvent saleAlert = new InventoryEvent();
                    saleAlert.productId = product.productId;
                    saleAlert.productName = product.name;
                    saleAlert.eventType = EventType.SALE_STARTED;
                    saleAlert.changeReason = String.format("%.0f%% OFF - Price dropped from $%.2f to $%.2f", 
                        discount, mainEvent.previousPrice, mainEvent.currentPrice);
                    saleAlert.timestamp = System.currentTimeMillis();
                    out.collect(saleAlert);
                }
            }
        }
    }
    
    /**
     * Setup Kafka sinks for inventory events
     */
    private static void setupKafkaSinks(DataStream<InventoryEvent> events, String bootstrapServers) {
        // Sink for inventory events topic
        KafkaSink<String> inventorySink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("inventory-events")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .build();
        
        // Send to inventory-events topic
        events
            .map(event -> MAPPER.writeValueAsString(event))
            .sinkTo(inventorySink)
            .name("Sink to inventory-events")
            .uid("inventory-events-sink");
        
        // Sink for WebSocket fanout
        KafkaSink<String> websocketSink = KafkaSink.<String>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("websocket_fanout")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .build();
        
        // Transform and send to WebSocket topic
        events
            .map(event -> {
                Map<String, Object> wsEvent = new HashMap<>();
                wsEvent.put("eventType", "INVENTORY_UPDATE");
                wsEvent.put("payload", event);
                wsEvent.put("timestamp", System.currentTimeMillis());
                return MAPPER.writeValueAsString(wsEvent);
            })
            .sinkTo(websocketSink)
            .name("Sink to websocket_fanout")
            .uid("websocket-fanout-sink");
        
        LOG.info("Configured Kafka sinks for inventory events");
    }
    
    /**
     * Event types for inventory management
     */
    public enum EventType {
        PRODUCT_ADDED,
        PRODUCT_UPDATED,
        INVENTORY_INCREASED,
        INVENTORY_DECREASED,
        OUT_OF_STOCK,
        BACK_IN_STOCK,
        PRICE_CHANGED,
        LOW_STOCK_ALERT,
        STALE_INVENTORY,
        SALE_STARTED
    }
    
    /**
     * Inventory event class with rich metadata
     */
    public static class InventoryEvent implements Serializable {
        public String productId;
        public String productName;
        public String category;
        public EventType eventType;
        public int previousInventory;
        public int currentInventory;
        public double previousPrice;
        public double currentPrice;
        public String changeReason;
        public long timestamp;
        public boolean isInitialLoad;
        public long millisSinceLastUpdate;
        public String alertLevel;
        public Product currentProduct;
        
        public InventoryEvent() {}
    }
    
    /**
     * Custom StreamFormat that reads entire file as a single String
     */
    public static class WholeFileStreamFormat extends SimpleStreamFormat<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public Reader createReader(Configuration config, FSDataInputStream stream) throws IOException {
            return new WholeFileReader(stream);
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
        
        private static class WholeFileReader implements Reader<String> {
            private final FSDataInputStream stream;
            private boolean hasRead = false;
            
            public WholeFileReader(FSDataInputStream stream) {
                this.stream = stream;
            }
            
            @Override
            public String read() throws IOException {
                if (hasRead) {
                    return null;
                }
                
                // Read entire file content
                StringBuilder content = new StringBuilder();
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = stream.read(buffer)) != -1) {
                    content.append(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
                }
                hasRead = true;
                return content.toString();
            }
            
            @Override
            public void close() throws IOException {
                stream.close();
            }
        }
    }
}