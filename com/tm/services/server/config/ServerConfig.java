package com.tm.services.server.config;

import com.tm.common.config.Config;

/** Typed server config; resolution (CLI flag &gt; env &gt; default) via {@link Config}. */
public final class ServerConfig {

    private final Config config;

    private ServerConfig(Config config) {
        this.config = config;
    }

    public static ServerConfig load(String[] args) {
        Config config = Config.from(args)
                .with("port", "PORT", "8080")
                .with("metrics-port", "METRICS_PORT", "9404")
                .with("redis-uri", "REDIS_URI", "redis://localhost:6379")
                .with("kafka-brokers", "KAFKA_BROKERS", "localhost:9092")
                .with("jdbc-url", "JDBC_URL", "jdbc:postgresql://localhost:5432/earlybird")
                .with("jdbc-user", "JDBC_USER", "earlybird")
                .with("jdbc-password", "JDBC_PASSWORD", "earlybird")
                .with("db-pool-size", "DB_POOL_SIZE", "4")
                .with("redis-warmup-interval-ms", "REDIS_WARMUP_INTERVAL_MS", "5000")
                .build();
        return new ServerConfig(config);
    }

    public int port() {
        return config.getInt("port");
    }

    /** Port for the Prometheus {@code /metrics} scrape endpoint. */
    public int metricsPort() {
        return config.getInt("metrics-port");
    }

    public String redisUri() {
        return config.get("redis-uri");
    }

    public String kafkaBrokers() {
        return config.get("kafka-brokers");
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

    /** Worker pool size for opportunity-CRUD JDBC ops. */
    public int dbPoolSize() {
        return config.getInt("db-pool-size");
    }

    /**
     * How often {@link com.tm.services.server.redis.RedisWarmupService} checks
     * open opportunities and restores {@code opp_meta}/{@code claimed_set} in
     * Redis if missing (e.g. after a Redis restart wiped its dataset).
     */
    public long redisWarmupIntervalMs() {
        return config.getLong("redis-warmup-interval-ms");
    }
}
