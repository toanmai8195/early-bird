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
 *     --db-pool-size=8
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
                .with("group-id", "GROUP_ID", "claim-manager")
                .with("topic", "TOPIC", "claim-events")
                .with("poll-timeout-ms", "POLL_TIMEOUT_MS", "500")
                .with("max-poll-records", "MAX_POLL_RECORDS", "500")
                .with("jdbc-url", "JDBC_URL", "jdbc:postgresql://localhost:5432/earlybird")
                .with("jdbc-user", "JDBC_USER", "earlybird")
                .with("jdbc-password", "JDBC_PASSWORD", "earlybird")
                .with("db-pool-size", "DB_POOL_SIZE", "8")
                .build();
        return new ManagerConfig(config);
    }

    public String kafkaBrokers() {
        return config.get("kafka-brokers");
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
}
