package com.ververica.composable_job.flink.inventory.shared.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.composable_job.flink.inventory.shared.config.InventoryConfig;
import com.ververica.composable_job.flink.inventory.shared.config.KafkaTopics;
import com.ververica.composable_job.flink.inventory.shared.model.AlertEvent;
import com.ververica.composable_job.flink.inventory.shared.model.InventoryEvent;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating Kafka sinks with proper configuration.
 *
 * PATTERN: Factory Pattern for Sink Creation
 *
 * Provides static factory methods to create different types of Kafka sinks:
 * - Inventory events sink
 * - Alerts sink
 * - WebSocket fanout sink
 * - Product updates sink
 *
 * Each sink is configured with:
 * - Appropriate topic from KafkaTopics
 * - Correct serialization
 * - Bootstrap servers from InventoryConfig
 *
 * USAGE:
 * <pre>
 * InventoryConfig config = InventoryConfig.fromEnvironment();
 * DataStream<InventoryEvent> events = ...;
 *
 * events
 *     .map(event -> mapper.writeValueAsString(event))
 *     .sinkTo(SinkFactory.createInventoryEventsSink(config))
 *     .name("Inventory Events Sink");
 * </pre>
 */
public class SinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(SinkFactory.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Private constructor to prevent instantiation
    private SinkFactory() {
        throw new UnsupportedOperationException("Utility class - do not instantiate");
    }

    /**
     * Creates a Kafka sink for inventory events.
     *
     * Topic: inventory-events
     * Purpose: Main stream of all inventory changes
     */
    public static KafkaSink<String> createInventoryEventsSink(InventoryConfig config) {
        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setKafkaProducerConfig(config.getKafkaPropertiesAsProperties())
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(KafkaTopics.INVENTORY_EVENTS)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .build();

        LOG.info("Created Kafka sink for topic: {}", KafkaTopics.INVENTORY_EVENTS);
        return sink;
    }

    /**
     * Creates a Kafka sink for alerts (low stock, out of stock, price drops).
     *
     * Topic: inventory-alerts
     * Purpose: Critical alerts requiring immediate attention
     */
    public static KafkaSink<String> createAlertsSink(InventoryConfig config) {
        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setKafkaProducerConfig(config.getKafkaPropertiesAsProperties())
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(KafkaTopics.INVENTORY_ALERTS)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .build();

        LOG.info("Created Kafka sink for topic: {}", KafkaTopics.INVENTORY_ALERTS);
        return sink;
    }

    /**
     * Creates a Kafka sink for WebSocket fanout.
     *
     * Topic: websocket_fanout
     * Purpose: Real-time updates to frontend via WebSocket
     *
     * Wraps events in a standard format:
     * {
     *   "eventType": "INVENTORY_UPDATE",
     *   "payload": {...},
     *   "timestamp": 1234567890
     * }
     */
    public static KafkaSink<String> createWebSocketSink(InventoryConfig config) {
        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setKafkaProducerConfig(config.getKafkaPropertiesAsProperties())
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(KafkaTopics.WEBSOCKET_FANOUT)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .build();

        LOG.info("Created Kafka sink for topic: {}", KafkaTopics.WEBSOCKET_FANOUT);
        return sink;
    }

    /**
     * Creates a Kafka sink for product updates.
     *
     * Topic: products
     * Purpose: Product catalog updates for downstream consumers
     */
    public static KafkaSink<String> createProductsSink(InventoryConfig config) {
        KafkaSink<String> sink = KafkaSink.<String>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setKafkaProducerConfig(config.getKafkaPropertiesAsProperties())
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(KafkaTopics.PRODUCTS)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .build();

        LOG.info("Created Kafka sink for topic: {}", KafkaTopics.PRODUCTS);
        return sink;
    }

    /**
     * Helper method: Sink inventory events with automatic JSON serialization.
     *
     * Combines the event stream → JSON → Kafka sink pipeline.
     */
    public static void sinkInventoryEvents(
            DataStream<InventoryEvent> events,
            InventoryConfig config) {

        events
            .map(event -> MAPPER.writeValueAsString(event))
            .sinkTo(createInventoryEventsSink(config))
            .name("Inventory Events to Kafka")
            .uid("inventory-events-sink");

        LOG.info("✅ Configured inventory events sink");
    }

    /**
     * Helper method: Sink alerts with automatic JSON serialization.
     */
    public static void sinkAlerts(
            DataStream<AlertEvent> alerts,
            InventoryConfig config) {

        alerts
            .map(alert -> MAPPER.writeValueAsString(alert))
            .sinkTo(createAlertsSink(config))
            .name("Alerts to Kafka")
            .uid("alerts-sink");

        LOG.info("✅ Configured alerts sink");
    }

    /**
     * Helper method: Sink inventory events to WebSocket fanout.
     *
     * Wraps events in WebSocket envelope format.
     */
    public static void sinkToWebSocket(
            DataStream<InventoryEvent> events,
            InventoryConfig config) {

        events
            .map(event -> {
                Map<String, Object> envelope = new HashMap<>();
                envelope.put("eventType", "INVENTORY_UPDATE");
                envelope.put("payload", event);
                envelope.put("timestamp", System.currentTimeMillis());
                return MAPPER.writeValueAsString(envelope);
            })
            .sinkTo(createWebSocketSink(config))
            .name("Inventory Events to WebSocket")
            .uid("websocket-sink");

        LOG.info("✅ Configured WebSocket fanout sink");
    }
}
