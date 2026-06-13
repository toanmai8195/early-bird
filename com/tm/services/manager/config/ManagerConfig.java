package com.tm.services.manager.config;

import com.tm.common.config.Config;

/**
 * Typed manager configuration. Declares its keys and delegates resolution
 * (CLI flag &gt; env &gt; default) to {@link Config}:
 *
 * <pre>
 *   manager \
 *     --kafka-brokers=broker:9092 \
 *     --group-id=claim-manager \
 *     --topic=claim-events \
 *     --poll-timeout-ms=500 \
 *     --max-poll-records=500 \
 *     --db-pool-size=16
 * </pre>
 */
public final class ManagerConfig {

    private final Config config;

    private ManagerConfig(Config config) {
        this.config = config;
    }

    public static ManagerConfig load(String[] args) {
        Config config = Config.from(args)
                .with("kafka-brokers", "KAFKA_BROKERS", "localhost:9092")
                .with("redis-uri", "REDIS_URI", "redis://localhost:6379")
                .with("metrics-port", "METRICS_PORT", "9404")
                .with("group-id", "GROUP_ID", "claim-manager")
                .with("topic", "TOPIC", "claim-events")
                .with("poll-timeout-ms", "POLL_TIMEOUT_MS", "500")
                .with("max-poll-records", "MAX_POLL_RECORDS", "2000")
                .with("settle-batch-size", "SETTLE_BATCH_SIZE", "200")
                .with("jdbc-url", "JDBC_URL", "jdbc:postgresql://localhost:5432/earlybird")
                .with("jdbc-user", "JDBC_USER", "earlybird")
                .with("jdbc-password", "JDBC_PASSWORD", "earlybird")
                .with("db-pool-size", "DB_POOL_SIZE", "16")
                .with("disable-circuit-breaker", "DISABLE_CIRCUIT_BREAKER", "false")
                .with("pg-probe-interval-ms", "PG_PROBE_INTERVAL_MS", "2000")
                .build();
        return new ManagerConfig(config);
    }

    public String kafkaBrokers() {
        return config.get("kafka-brokers");
    }

    /** Port for the Prometheus {@code /metrics} scrape endpoint. */
    public int metricsPort() {
        return config.getInt("metrics-port");
    }

    public String redisUri() {
        return config.get("redis-uri");
    }

    public String groupId() {
        return config.get("group-id");
    }

    public String topic() {
        return config.get("topic");
    }

    public long pollTimeoutMs() {
        return config.getLong("poll-timeout-ms");
    }

    public int maxPollRecords() {
        return config.getInt("max-poll-records");
    }

    /**
     * Max claims per CTE settle statement (one row-lock + round-trip per sub-batch).
     * For a single hot opp the settle serializes on the opportunities row, so larger
     * batches amortise lock+RTT overhead — the main throughput lever for the contended
     * pattern. Capped per opp; smaller opps (diverse pattern) settle in one statement.
     */
    public int settleBatchSize() {
        return config.getInt("settle-batch-size");
    }

    /** Worker pool size for parallel per-opportunity settle (also sizes the JDBC connection pool). */
    public int dbPoolSize() {
        return config.getInt("db-pool-size");
    }

    public String jdbcUrl() {
        return config.get("jdbc-url");
    }

    public String jdbcUser() {
        return config.get("jdbc-user");
    }

    public String jdbcPassword() {
        return config.get("jdbc-password");
    }

    /**
     * When true, the PG-settle circuit breaker is forced to DISABLED state (always
     * passes through to PG). The breaker is enabled by default; disable it for load
     * testing where tripping OPEN would mask raw PG throughput.
     */
    public boolean disableCircuitBreaker() {
        return config.getBool("disable-circuit-breaker");
    }

    /**
     * How often {@link com.tm.services.manager.PgHealthProbe} pings PG to drive
     * circuit-breaker recovery while it is OPEN. Should be ≤ the breaker's
     * open-state wait so recovery is detected promptly once PG is back.
     */
    public long pgProbeIntervalMs() {
        return config.getLong("pg-probe-interval-ms");
    }
}
