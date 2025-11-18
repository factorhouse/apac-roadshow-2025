package com.ververica.composable_job.flink.inventory.patterns.hybrid_source;

import com.ververica.composable_job.flink.inventory.shared.config.InventoryConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * FLINK PATTERN: Hybrid Source (Bounded → Unbounded)
 *
 * PURPOSE:
 * Bootstrap state from historical data (file) before processing real-time events (Kafka).
 * This is a common pattern for:
 * - Initializing lookup tables
 * - Preloading product catalogs
 * - Bootstrapping ML models
 * - Reprocessing historical data + continuing with live stream
 *
 * KEY CONCEPTS:
 * 1. Flink 1.20+ HybridSource seamlessly switches between sources
 * 2. First source is BOUNDED (file) - has a defined end
 * 3. Second source is UNBOUNDED (Kafka) - continues indefinitely
 * 4. No code changes needed when switching - completely transparent
 *
 * WHEN TO USE:
 * - Need to initialize state before processing live data
 * - Reprocessing scenario (replay file + continue with live)
 * - Testing (use file in dev, Kafka in prod)
 *
 * ALTERNATIVE APPROACHES:
 * - Two separate jobs (batch + streaming) - more complex state management
 * - Manual union of streams - requires careful watermark handling
 * - Kafka-only with large retention - wastes storage
 */
public class HybridSourceExample {

    private static final Logger LOG = LoggerFactory.getLogger(HybridSourceExample.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        InventoryConfig config = InventoryConfig.fromEnvironment();

        // Create hybrid source: File → Kafka
        HybridSource<String> hybridSource = createHybridSource(config);

        // Use hybrid source - downstream code doesn't know if data is from file or Kafka!
        DataStream<String> productStream = env.fromSource(
            hybridSource,
            WatermarkStrategy.noWatermarks(),
            "Product Hybrid Source",
            Types.STRING
        );

        // Process data (source is transparent)
        productStream
            .map(product -> {
                LOG.info("Processing product: {}", product.substring(0, Math.min(100, product.length())));
                return product;
            })
            .print();

        env.execute("Hybrid Source Pattern Example");
    }

    /**
     * Creates a Hybrid Source that:
     * 1. FIRST: Reads entire file as initial bootstrap data
     * 2. THEN: Automatically switches to Kafka for real-time updates
     *
     * The switch happens automatically when the file source completes.
     */
    public static HybridSource<String> createHybridSource(InventoryConfig config) {
        // STEP 1: Create BOUNDED file source for initial load
        FileSource<String> fileSource = FileSource
            .forRecordStreamFormat(
                new WholeFileStreamFormat(),  // Custom format that reads entire file at once
                new Path(config.getInitialProductsFile())
            )
            .build();

        // STEP 2: Create UNBOUNDED Kafka source for continuous updates
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers(config.getKafkaBootstrapServers())
            .setTopics(config.getProductUpdatesTopic())
            .setGroupId(config.getKafkaGroupId())
            .setStartingOffsets(OffsetsInitializer.latest())  // Only NEW messages after file
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperty("partition.discovery.interval.ms", "10000")  // Discover new partitions
            .setProperties(config.getKafkaPropertiesAsProperties())
            .build();

        // STEP 3: Build hybrid source
        // Order matters: file FIRST, then Kafka
        HybridSource<String> hybridSource = HybridSource.builder(fileSource)
            .addSource(kafkaSource)
            .build();

        LOG.info("✅ Hybrid Source created: {} (file) → {} (Kafka)", config.getInitialProductsFile(), config.getKafkaBootstrapServers());
        LOG.info("📖 Will first load all products from file, then switch to live Kafka stream");

        return hybridSource;
    }

    /**
     * Custom StreamFormat that reads an entire file as a single record.
     *
     * WHY: JSON arrays need to be read completely, not line-by-line.
     * This format reads the entire file content at once.
     *
     * For line-delimited JSON, use TextLineInputFormat instead.
     */
    public static class WholeFileStreamFormat extends SimpleStreamFormat<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public Reader createReader(Configuration config, FSDataInputStream stream) {
            return new WholeFileReader(stream);
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }

        /**
         * Reader that consumes entire file in one read.
         * Returns null on second call to signal end-of-file.
         */
        private static class WholeFileReader implements Reader<String> {
            private final FSDataInputStream stream;
            private boolean hasRead = false;

            public WholeFileReader(FSDataInputStream stream) {
                this.stream = stream;
            }

            @Override
            public String read() throws IOException {
                if (hasRead) {
                    return null;  // Signal end of file
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
