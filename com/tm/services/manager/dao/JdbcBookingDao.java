package com.tm.services.manager.dao;

import com.tm.common.kafka.ClaimEvent;
import com.tm.common.metric.Metrics;
import com.tm.common.pg.ClaimStore;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;

import java.sql.Connection;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

/**
 * JDBC {@link BookingDao}. Each opportunity is settled with ONE bulk statement
 * (see {@link ClaimStore}) on its own connection, dispatched to a Vert.x worker
 * pool so opportunities run in parallel and the caller can react to each one
 * as soon as it completes (instead of waiting for the whole poll batch).
 *
 * <p>See .claude/rules/observability-metrics.md — every DAO func records a
 * counter and a P99 latency timer.
 */
@Singleton
public final class JdbcBookingDao implements BookingDao {

    private static final String METRIC = "booking.dao.commit";
    private static final String LATENCY = "booking.dao.commit.latency";

    private final DataSource dataSource;
    private final ClaimStore claimStore;
    private final Metrics metrics;
    private final WorkerExecutor workerExecutor;

    @Inject
    public JdbcBookingDao(DataSource dataSource, ClaimStore claimStore, Metrics metrics,
                          WorkerExecutor workerExecutor) {
        this.dataSource = dataSource;
        this.claimStore = claimStore;
        this.metrics = metrics;
        this.workerExecutor = workerExecutor;
    }

    @Override
    public Future<List<ClaimStore.Outcome>> settleOpportunity(String opportunityId, List<ClaimEvent> events) {
        Timer.Sample sample = Timer.start();
        return workerExecutor.executeBlocking(() -> {
            try (Connection conn = dataSource.getConnection()) {
                // One statement per connection -> autocommit (default) is fine,
                // Postgres commits it atomically on its own.
                return claimStore.settleOpportunity(conn, opportunityId, events);
            }
        }, false).onComplete(ar -> {
            sample.stop(metrics.timer(LATENCY));
            if (ar.succeeded()) {
                for (ClaimStore.Outcome o : ar.result()) {
                    metrics.counter(METRIC, o.name().toLowerCase()).increment();
                }
            } else {
                metrics.counter(METRIC, "error").increment();
            }
        });
    }
}
