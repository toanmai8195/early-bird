package com.tm.common.pg;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

/**
 * Shared Postgres connection-pool factory. Returns a pooled {@link DataSource}
 * so callers borrow/return connections instead of opening a fresh physical
 * socket per query — without pooling the open/close churn piles up TIME_WAIT
 * sockets and exhausts ephemeral ports under load
 * ({@code java.net.BindException: Cannot assign requested address}).
 *
 * <p>Mirrors {@link com.tm.common.kafka.KafkaClients}: a static factory for an
 * external client, returning the {@link DataSource} interface (HikariCP is an
 * impl detail callers never see).
 */
public final class PgPool {

    private PgPool() {}

    /**
     * Pooled {@link DataSource}. Size {@code maxPoolSize} to the caller's
     * worker/thread pool that runs blocking queries, so every worker has a
     * connection available.
     */
    public static DataSource dataSource(String jdbcUrl, String user, String password,
                                        int maxPoolSize, String poolName) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(jdbcUrl);
        hc.setUsername(user);
        hc.setPassword(password);
        hc.setMaximumPoolSize(maxPoolSize);
        // Fixed, fully-warm pool: this is a steady high-throughput workload, so
        // keep every connection open (minIdle == max) to avoid connect-latency
        // spikes mid-load instead of growing/shrinking the pool.
        hc.setMinimumIdle(maxPoolSize);
        // Each settle is one autocommit statement (PgClaimStore); the pool need
        // not manage transactions, just hand out warm connections.
        hc.setAutoCommit(true);
        // Fail fast when the pool is saturated so the caller sees backpressure
        // (and the consumer slows) instead of threads parking for 30s (default).
        hc.setConnectionTimeout(5_000);
        hc.setPoolName(poolName);
        return new HikariDataSource(hc);
    }
}
