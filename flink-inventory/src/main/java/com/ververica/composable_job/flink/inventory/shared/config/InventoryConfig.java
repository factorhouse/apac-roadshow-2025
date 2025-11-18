package com.ververica.composable_job.flink.inventory.shared.config;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Main configuration class for the Inventory Management Job.
 *
 * <p>This class provides a comprehensive configuration for the Flink inventory job,
 * including Kafka settings, file paths, checkpoint configuration, and parallelism settings.
 * It uses the builder pattern for easy and flexible configuration.</p>
 *
 * <p>Example usage:
 * <pre>{@code
 * // From environment variables
 * InventoryConfig config = InventoryConfig.fromEnvironment();
 *
 * // From command line arguments
 * InventoryConfig config = InventoryConfig.fromArgs(args);
 *
 * // Custom configuration
 * InventoryConfig config = InventoryConfig.builder()
 *     .withKafkaBootstrapServers("localhost:19092")
 *     .withInitialProductsFile("data/products.json")
 *     .withParallelism(4)
 *     .withCheckpointInterval(60000L)
 *     .build();
 * }</pre>
 *
 * @author Ververica
 * @since 1.0
 */
public class InventoryConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // Kafka Configuration
    private final String kafkaBootstrapServers;
    private final String kafkaGroupId;
    private final Map<String, String> kafkaProperties;

    // Topic Names
    private final String productsTopic;
    private final String productUpdatesTopic;
    private final String inventoryEventsTopic;
    private final String inventoryAlertsTopic;
    private final String websocketFanoutTopic;

    // File Paths
    private final String initialProductsFile;
    private final String checkpointPath;

    // Checkpoint Configuration
    private final long checkpointInterval;
    private final long checkpointTimeout;
    private final long minPauseBetweenCheckpoints;
    private final int maxConcurrentCheckpoints;
    private final boolean enableExternalizedCheckpoints;

    // Parallelism Settings
    private final int parallelism;
    private final int maxParallelism;

    // State Configuration
    private final StateConfig stateConfig;

    // Job Settings
    private final String jobName;
    private final boolean enableObjectReuse;
    private final long partitionDiscoveryIntervalMs;

    private InventoryConfig(Builder builder) {
        this.kafkaBootstrapServers = builder.kafkaBootstrapServers;
        this.kafkaGroupId = builder.kafkaGroupId;
        this.kafkaProperties = new HashMap<>(builder.kafkaProperties);
        this.productsTopic = builder.productsTopic;
        this.productUpdatesTopic = builder.productUpdatesTopic;
        this.inventoryEventsTopic = builder.inventoryEventsTopic;
        this.inventoryAlertsTopic = builder.inventoryAlertsTopic;
        this.websocketFanoutTopic = builder.websocketFanoutTopic;
        this.initialProductsFile = builder.initialProductsFile;
        this.checkpointPath = builder.checkpointPath;
        this.checkpointInterval = builder.checkpointInterval;
        this.checkpointTimeout = builder.checkpointTimeout;
        this.minPauseBetweenCheckpoints = builder.minPauseBetweenCheckpoints;
        this.maxConcurrentCheckpoints = builder.maxConcurrentCheckpoints;
        this.enableExternalizedCheckpoints = builder.enableExternalizedCheckpoints;
        this.parallelism = builder.parallelism;
        this.maxParallelism = builder.maxParallelism;
        this.stateConfig = builder.stateConfig;
        this.jobName = builder.jobName;
        this.enableObjectReuse = builder.enableObjectReuse;
        this.partitionDiscoveryIntervalMs = builder.partitionDiscoveryIntervalMs;
    }

    /**
     * Creates a new builder with default values.
     *
     * @return a new InventoryConfig builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates an InventoryConfig from environment variables.
     *
     * <p>Reads the following environment variables:
     * <ul>
     *   <li>KAFKA_BOOTSTRAP_SERVERS - Kafka bootstrap servers (default: localhost:19092)</li>
     *   <li>KAFKA_GROUP_ID - Kafka consumer group ID (default: inventory-management-hybrid)</li>
     *   <li>INITIAL_PRODUCTS_FILE - Path to initial products file (default: data/initial-products.json)</li>
     *   <li>CHECKPOINT_PATH - Checkpoint storage path (default: file:///tmp/flink-checkpoints)</li>
     *   <li>CHECKPOINT_INTERVAL_MS - Checkpoint interval in ms (default: 60000)</li>
     *   <li>PARALLELISM - Job parallelism (default: 2)</li>
     *   <li>JOB_NAME - Job name (default: Inventory Management with Hybrid Source)</li>
     * </ul>
     *
     * @return InventoryConfig initialized from environment
     */
    public static InventoryConfig fromEnvironment() {
        Builder builder = builder()
            .withKafkaBootstrapServers(getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
            .withKafkaGroupId(getEnv("KAFKA_GROUP_ID", "inventory-management-hybrid"))
            .withInitialProductsFile(getEnv("INITIAL_PRODUCTS_FILE", "data/initial-products.json"))
            .withCheckpointPath(getEnv("CHECKPOINT_PATH", "file:///tmp/flink-checkpoints"))
            .withCheckpointInterval(Long.parseLong(getEnv("CHECKPOINT_INTERVAL_MS", "60000")))
            .withParallelism(Integer.parseInt(getEnv("PARALLELISM", "2")))
            .withJobName(getEnv("JOB_NAME", "Inventory Management with Hybrid Source"));

        // Automatically add security config if env vars are set
        String kafkaUser = System.getenv("KAFKA_USER");
        String kafkaPassword = System.getenv("KAFKA_PASSWORD");

        if (kafkaUser != null && !kafkaUser.isEmpty() && kafkaPassword != null && !kafkaPassword.isEmpty()) {
            System.out.println("🔐 Security variables detected in InventoryConfig. Configuring SASL/SCRAM.");
            String jaasConfig = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                kafkaUser,
                kafkaPassword
            );
            builder.withKafkaProperty("security.protocol", "SASL_PLAINTEXT");
            builder.withKafkaProperty("sasl.mechanism", "SCRAM-SHA-256");
            builder.withKafkaProperty("sasl.jaas.config", jaasConfig);
        } else {
             System.out.println("⚪ No security variables found in InventoryConfig. Assuming local, unsecured Kafka.");
        }

        return builder.build();
    }

    /**
     * Creates an InventoryConfig from command line arguments.
     *
     * <p>Expected argument format: --key value
     * <p>Supported arguments:
     * <ul>
     *   <li>--kafka-bootstrap-servers - Kafka bootstrap servers</li>
     *   <li>--kafka-group-id - Kafka consumer group ID</li>
     *   <li>--initial-products-file - Path to initial products file</li>
     *   <li>--checkpoint-path - Checkpoint storage path</li>
     *   <li>--checkpoint-interval - Checkpoint interval in ms</li>
     *   <li>--parallelism - Job parallelism</li>
     *   <li>--job-name - Job name</li>
     * </ul>
     *
     * @param args command line arguments
     * @return InventoryConfig initialized from arguments, with defaults from environment
     */
    public static InventoryConfig fromArgs(String[] args) {
        Map<String, String> argMap = parseArgs(args);

        return builder()
            .withKafkaBootstrapServers(argMap.getOrDefault("kafka-bootstrap-servers",
                getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")))
            .withKafkaGroupId(argMap.getOrDefault("kafka-group-id",
                getEnv("KAFKA_GROUP_ID", "inventory-management-hybrid")))
            .withInitialProductsFile(argMap.getOrDefault("initial-products-file",
                getEnv("INITIAL_PRODUCTS_FILE", "data/initial-products.json")))
            .withCheckpointPath(argMap.getOrDefault("checkpoint-path",
                getEnv("CHECKPOINT_PATH", "file:///tmp/flink-checkpoints")))
            .withCheckpointInterval(Long.parseLong(argMap.getOrDefault("checkpoint-interval",
                getEnv("CHECKPOINT_INTERVAL_MS", "60000"))))
            .withParallelism(Integer.parseInt(argMap.getOrDefault("parallelism",
                getEnv("PARALLELISM", "2"))))
            .withJobName(argMap.getOrDefault("job-name",
                getEnv("JOB_NAME", "Inventory Management with Hybrid Source")))
            .build();
    }

    /**
     * Validates the configuration and throws an exception if invalid.
     *
     * @throws IllegalStateException if configuration is invalid
     */
    public void validate() {
        if (kafkaBootstrapServers == null || kafkaBootstrapServers.trim().isEmpty()) {
            throw new IllegalStateException("Kafka bootstrap servers cannot be null or empty");
        }
        if (kafkaGroupId == null || kafkaGroupId.trim().isEmpty()) {
            throw new IllegalStateException("Kafka group ID cannot be null or empty");
        }
        if (initialProductsFile == null || initialProductsFile.trim().isEmpty()) {
            throw new IllegalStateException("Initial products file cannot be null or empty");
        }
        if (checkpointInterval <= 0) {
            throw new IllegalStateException("Checkpoint interval must be positive");
        }
        if (parallelism <= 0) {
            throw new IllegalStateException("Parallelism must be positive");
        }
        if (maxParallelism < parallelism) {
            throw new IllegalStateException("Max parallelism must be greater than or equal to parallelism");
        }
        if (stateConfig != null) {
            // StateConfig has its own validation
        }
    }

    // Getters
    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public Map<String, String> getKafkaProperties() {
        return new HashMap<>(kafkaProperties);
    }

    public String getProductsTopic() {
        return productsTopic;
    }

    public String getProductUpdatesTopic() {
        return productUpdatesTopic;
    }

    public String getInventoryEventsTopic() {
        return inventoryEventsTopic;
    }

    public String getInventoryAlertsTopic() {
        return inventoryAlertsTopic;
    }

    public String getWebsocketFanoutTopic() {
        return websocketFanoutTopic;
    }

    public String getInitialProductsFile() {
        return initialProductsFile;
    }

    public String getCheckpointPath() {
        return checkpointPath;
    }

    public long getCheckpointInterval() {
        return checkpointInterval;
    }

    public long getCheckpointTimeout() {
        return checkpointTimeout;
    }

    public long getMinPauseBetweenCheckpoints() {
        return minPauseBetweenCheckpoints;
    }

    public int getMaxConcurrentCheckpoints() {
        return maxConcurrentCheckpoints;
    }

    public boolean isEnableExternalizedCheckpoints() {
        return enableExternalizedCheckpoints;
    }

    public int getParallelism() {
        return parallelism;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public StateConfig getStateConfig() {
        return stateConfig;
    }

    public String getJobName() {
        return jobName;
    }

    public boolean isEnableObjectReuse() {
        return enableObjectReuse;
    }

    public long getPartitionDiscoveryIntervalMs() {
        return partitionDiscoveryIntervalMs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InventoryConfig that = (InventoryConfig) o;
        return checkpointInterval == that.checkpointInterval &&
               checkpointTimeout == that.checkpointTimeout &&
               minPauseBetweenCheckpoints == that.minPauseBetweenCheckpoints &&
               maxConcurrentCheckpoints == that.maxConcurrentCheckpoints &&
               enableExternalizedCheckpoints == that.enableExternalizedCheckpoints &&
               parallelism == that.parallelism &&
               maxParallelism == that.maxParallelism &&
               enableObjectReuse == that.enableObjectReuse &&
               partitionDiscoveryIntervalMs == that.partitionDiscoveryIntervalMs &&
               Objects.equals(kafkaBootstrapServers, that.kafkaBootstrapServers) &&
               Objects.equals(kafkaGroupId, that.kafkaGroupId) &&
               Objects.equals(kafkaProperties, that.kafkaProperties) &&
               Objects.equals(productsTopic, that.productsTopic) &&
               Objects.equals(productUpdatesTopic, that.productUpdatesTopic) &&
               Objects.equals(inventoryEventsTopic, that.inventoryEventsTopic) &&
               Objects.equals(inventoryAlertsTopic, that.inventoryAlertsTopic) &&
               Objects.equals(websocketFanoutTopic, that.websocketFanoutTopic) &&
               Objects.equals(initialProductsFile, that.initialProductsFile) &&
               Objects.equals(checkpointPath, that.checkpointPath) &&
               Objects.equals(stateConfig, that.stateConfig) &&
               Objects.equals(jobName, that.jobName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kafkaBootstrapServers, kafkaGroupId, kafkaProperties,
                          productsTopic, productUpdatesTopic, inventoryEventsTopic,
                          inventoryAlertsTopic, websocketFanoutTopic, initialProductsFile,
                          checkpointPath, checkpointInterval, checkpointTimeout,
                          minPauseBetweenCheckpoints, maxConcurrentCheckpoints,
                          enableExternalizedCheckpoints, parallelism, maxParallelism,
                          stateConfig, jobName, enableObjectReuse, partitionDiscoveryIntervalMs);
    }

    @Override
    public String toString() {
        return "InventoryConfig{" +
               "kafkaBootstrapServers='" + kafkaBootstrapServers + '\'' +
               ", kafkaGroupId='" + kafkaGroupId + '\'' +
               ", productsTopic='" + productsTopic + '\'' +
               ", productUpdatesTopic='" + productUpdatesTopic + '\'' +
               ", inventoryEventsTopic='" + inventoryEventsTopic + '\'' +
               ", inventoryAlertsTopic='" + inventoryAlertsTopic + '\'' +
               ", websocketFanoutTopic='" + websocketFanoutTopic + '\'' +
               ", initialProductsFile='" + initialProductsFile + '\'' +
               ", checkpointPath='" + checkpointPath + '\'' +
               ", checkpointInterval=" + checkpointInterval +
               ", checkpointTimeout=" + checkpointTimeout +
               ", minPauseBetweenCheckpoints=" + minPauseBetweenCheckpoints +
               ", maxConcurrentCheckpoints=" + maxConcurrentCheckpoints +
               ", enableExternalizedCheckpoints=" + enableExternalizedCheckpoints +
               ", parallelism=" + parallelism +
               ", maxParallelism=" + maxParallelism +
               ", stateConfig=" + stateConfig +
               ", jobName='" + jobName + '\'' +
               ", enableObjectReuse=" + enableObjectReuse +
               ", partitionDiscoveryIntervalMs=" + partitionDiscoveryIntervalMs +
               '}';
    }

    // Helper methods
    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> argMap = new HashMap<>();
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].startsWith("--")) {
                String key = args[i].substring(2);
                String value = args[i + 1];
                argMap.put(key, value);
                i++; // Skip the value in the next iteration
            }
        }
        return argMap;
    }

    /**
     * Builder for InventoryConfig with fluent API.
     */
    public static class Builder {
        // Kafka Configuration - defaults
        private String kafkaBootstrapServers = "localhost:19092";
        private String kafkaGroupId = "inventory-management-hybrid";
        private Map<String, String> kafkaProperties = new HashMap<>();

        // Topic Names - defaults from KafkaTopics
        private String productsTopic = KafkaTopics.PRODUCTS;
        private String productUpdatesTopic = KafkaTopics.PRODUCT_UPDATES;
        private String inventoryEventsTopic = KafkaTopics.INVENTORY_EVENTS;
        private String inventoryAlertsTopic = KafkaTopics.INVENTORY_ALERTS;
        private String websocketFanoutTopic = KafkaTopics.WEBSOCKET_FANOUT;

        // File Paths - defaults
        private String initialProductsFile = "data/initial-products.json";
        private String checkpointPath = "file:///tmp/flink-checkpoints";

        // Checkpoint Configuration - defaults
        private long checkpointInterval = 60000L; // 1 minute
        private long checkpointTimeout = 600000L; // 10 minutes
        private long minPauseBetweenCheckpoints = 30000L; // 30 seconds
        private int maxConcurrentCheckpoints = 1;
        private boolean enableExternalizedCheckpoints = true;

        // Parallelism Settings - defaults
        private int parallelism = 2;
        private int maxParallelism = 128;

        // State Configuration - default
        private StateConfig stateConfig = StateConfig.withDefaults();

        // Job Settings - defaults
        private String jobName = "Inventory Management with Hybrid Source";
        private boolean enableObjectReuse = false;
        private long partitionDiscoveryIntervalMs = 10000L; // 10 seconds

        private Builder() {
            // Set default Kafka properties
            kafkaProperties.put("partition.discovery.interval.ms", "10000");
            kafkaProperties.put("commit.offsets.on.checkpoint", "true");
        }

        // Kafka Configuration
        public Builder withKafkaBootstrapServers(String kafkaBootstrapServers) {
            this.kafkaBootstrapServers = Objects.requireNonNull(kafkaBootstrapServers, "kafkaBootstrapServers cannot be null");
            return this;
        }

        public Builder withKafkaGroupId(String kafkaGroupId) {
            this.kafkaGroupId = Objects.requireNonNull(kafkaGroupId, "kafkaGroupId cannot be null");
            return this;
        }

        public Builder withKafkaProperty(String key, String value) {
            this.kafkaProperties.put(
                Objects.requireNonNull(key, "key cannot be null"),
                Objects.requireNonNull(value, "value cannot be null")
            );
            return this;
        }

        public Builder withKafkaProperties(Map<String, String> properties) {
            this.kafkaProperties.putAll(Objects.requireNonNull(properties, "properties cannot be null"));
            return this;
        }

        // Topic Names
        public Builder withProductsTopic(String productsTopic) {
            this.productsTopic = Objects.requireNonNull(productsTopic, "productsTopic cannot be null");
            return this;
        }

        public Builder withProductUpdatesTopic(String productUpdatesTopic) {
            this.productUpdatesTopic = Objects.requireNonNull(productUpdatesTopic, "productUpdatesTopic cannot be null");
            return this;
        }

        public Builder withInventoryEventsTopic(String inventoryEventsTopic) {
            this.inventoryEventsTopic = Objects.requireNonNull(inventoryEventsTopic, "inventoryEventsTopic cannot be null");
            return this;
        }

        public Builder withInventoryAlertsTopic(String inventoryAlertsTopic) {
            this.inventoryAlertsTopic = Objects.requireNonNull(inventoryAlertsTopic, "inventoryAlertsTopic cannot be null");
            return this;
        }

        public Builder withWebsocketFanoutTopic(String websocketFanoutTopic) {
            this.websocketFanoutTopic = Objects.requireNonNull(websocketFanoutTopic, "websocketFanoutTopic cannot be null");
            return this;
        }

        // File Paths
        public Builder withInitialProductsFile(String initialProductsFile) {
            this.initialProductsFile = Objects.requireNonNull(initialProductsFile, "initialProductsFile cannot be null");
            return this;
        }

        public Builder withCheckpointPath(String checkpointPath) {
            this.checkpointPath = Objects.requireNonNull(checkpointPath, "checkpointPath cannot be null");
            return this;
        }

        // Checkpoint Configuration
        public Builder withCheckpointInterval(long checkpointInterval) {
            if (checkpointInterval <= 0) {
                throw new IllegalArgumentException("checkpointInterval must be positive");
            }
            this.checkpointInterval = checkpointInterval;
            return this;
        }

        public Builder withCheckpointTimeout(long checkpointTimeout) {
            if (checkpointTimeout <= 0) {
                throw new IllegalArgumentException("checkpointTimeout must be positive");
            }
            this.checkpointTimeout = checkpointTimeout;
            return this;
        }

        public Builder withMinPauseBetweenCheckpoints(long minPauseBetweenCheckpoints) {
            if (minPauseBetweenCheckpoints < 0) {
                throw new IllegalArgumentException("minPauseBetweenCheckpoints cannot be negative");
            }
            this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
            return this;
        }

        public Builder withMaxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
            if (maxConcurrentCheckpoints <= 0) {
                throw new IllegalArgumentException("maxConcurrentCheckpoints must be positive");
            }
            this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
            return this;
        }

        public Builder withEnableExternalizedCheckpoints(boolean enableExternalizedCheckpoints) {
            this.enableExternalizedCheckpoints = enableExternalizedCheckpoints;
            return this;
        }

        // Parallelism Settings
        public Builder withParallelism(int parallelism) {
            if (parallelism <= 0) {
                throw new IllegalArgumentException("parallelism must be positive");
            }
            this.parallelism = parallelism;
            return this;
        }

        public Builder withMaxParallelism(int maxParallelism) {
            if (maxParallelism <= 0) {
                throw new IllegalArgumentException("maxParallelism must be positive");
            }
            this.maxParallelism = maxParallelism;
            return this;
        }

        // State Configuration
        public Builder withStateConfig(StateConfig stateConfig) {
            this.stateConfig = Objects.requireNonNull(stateConfig, "stateConfig cannot be null");
            return this;
        }

        // Job Settings
        public Builder withJobName(String jobName) {
            this.jobName = Objects.requireNonNull(jobName, "jobName cannot be null");
            return this;
        }

        public Builder withEnableObjectReuse(boolean enableObjectReuse) {
            this.enableObjectReuse = enableObjectReuse;
            return this;
        }

        public Builder withPartitionDiscoveryIntervalMs(long partitionDiscoveryIntervalMs) {
            if (partitionDiscoveryIntervalMs <= 0) {
                throw new IllegalArgumentException("partitionDiscoveryIntervalMs must be positive");
            }
            this.partitionDiscoveryIntervalMs = partitionDiscoveryIntervalMs;
            return this;
        }

        /**
         * Builds and validates the InventoryConfig instance.
         *
         * @return a new InventoryConfig
         * @throws IllegalStateException if configuration is invalid
         */
        public InventoryConfig build() {
            InventoryConfig config = new InventoryConfig(this);
            config.validate();
            return config;
        }
    }

    /**
     * Returns the Kafka properties as a java.util.Properties object,
     * which is required by the Flink Kafka connectors.
     *
     * @return Kafka properties as a Properties object.
     */
    public Properties getKafkaPropertiesAsProperties() {
        Properties props = new Properties();
        props.putAll(this.kafkaProperties);
        return props;
    }
}
