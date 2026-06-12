package com.tm.services.server.dao;

import com.tm.common.metric.Metrics;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import io.vertx.redis.client.RedisAPI;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

/**
 * JDBC {@link OpportunityDao}. Each CRUD op runs its own JDBC statement on a
 * worker pool (off the event loop), then syncs {@code opp_meta:{opp}} in Redis
 * so the claim-path gate stays in sync with PG.
 *
 * <p>See .claude/rules/observability-metrics.md — every DAO func records a
 * counter and a P99 latency timer.
 */
@Singleton
public final class JdbcOpportunityDao implements OpportunityDao {

    private static final String METRIC = "booking.dao.opportunity";
    private static final String LATENCY = "booking.dao.opportunity.latency";

    private static final String INSERT_SQL = """
            INSERT INTO opportunities
              (opportunity_id, region_id, zone_id, booking_window_start, booking_window_end, capacity, remaining)
            VALUES (?, ?, ?, to_timestamp(?::double precision), to_timestamp(?::double precision), ?, ?)
            """;

    private static final String SELECT_SQL = """
            SELECT region_id, zone_id,
                   extract(epoch FROM booking_window_start)::bigint,
                   extract(epoch FROM booking_window_end)::bigint,
                   capacity, remaining
            FROM opportunities WHERE opportunity_id = ?
            """;

    private static final String UPDATE_SQL = """
            UPDATE opportunities
            SET region_id = ?, zone_id = ?,
                booking_window_start = to_timestamp(?::double precision),
                booking_window_end = to_timestamp(?::double precision),
                capacity = ?, remaining = ?
            WHERE opportunity_id = ?
            RETURNING region_id, zone_id,
                      extract(epoch FROM booking_window_start)::bigint,
                      extract(epoch FROM booking_window_end)::bigint,
                      capacity, remaining
            """;

    private static final String DELETE_SQL = "DELETE FROM opportunities WHERE opportunity_id = ?";

    private final DataSource dataSource;
    private final RedisAPI redis;
    private final Metrics metrics;
    private final WorkerExecutor workerExecutor;

    @Inject
    public JdbcOpportunityDao(DataSource dataSource, RedisAPI redis, Metrics metrics, WorkerExecutor workerExecutor) {
        this.dataSource = dataSource;
        this.redis = redis;
        this.metrics = metrics;
        this.workerExecutor = workerExecutor;
    }

    @Override
    public Future<Opportunity> create(Opportunity opportunity) {
        Timer.Sample sample = Timer.start();
        return workerExecutor.executeBlocking(() -> insert(opportunity), false)
                .compose(this::syncMeta)
                .andThen(ar -> record(sample, "create", ar.succeeded()));
    }

    @Override
    public Future<Optional<Opportunity>> get(String opportunityId) {
        Timer.Sample sample = Timer.start();
        return workerExecutor.executeBlocking(() -> select(opportunityId), false)
                .andThen(ar -> record(sample, "get", ar.succeeded()));
    }

    @Override
    public Future<Optional<Opportunity>> update(String opportunityId, Opportunity opportunity) {
        Timer.Sample sample = Timer.start();
        return workerExecutor.executeBlocking(() -> updateRow(opportunityId, opportunity), false)
                .compose(updated -> updated.isEmpty()
                        ? Future.succeededFuture(Optional.<Opportunity>empty())
                        : syncMeta(updated.get()).map(Optional::of))
                .andThen(ar -> record(sample, "update", ar.succeeded()));
    }

    @Override
    public Future<Boolean> delete(String opportunityId) {
        Timer.Sample sample = Timer.start();
        return workerExecutor.executeBlocking(() -> deleteRow(opportunityId), false)
                .compose(deleted -> deleted ? clearMeta(opportunityId).map(v -> true) : Future.succeededFuture(false))
                .andThen(ar -> record(sample, "delete", ar.succeeded()));
    }

    private void record(Timer.Sample sample, String op, boolean ok) {
        sample.stop(metrics.timer(LATENCY));
        metrics.counter(METRIC, ok ? op + "_ok" : op + "_error").increment();
    }

    /** Writes capacity + window_start to opp_meta:{opp} and TTLs the key to window_end. */
    private Future<Opportunity> syncMeta(Opportunity o) {
        String metaKey = "opp_meta:" + o.opportunityId();
        return redis.hset(List.of(metaKey,
                        "capacity", Integer.toString(o.capacity()),
                        "window_start", Long.toString(o.bookingWindowStart())))
                .compose(v -> redis.expireat(List.of(metaKey, Long.toString(o.bookingWindowEnd()))))
                .map(v -> o);
    }

    private Future<Void> clearMeta(String opportunityId) {
        return redis.del(List.of("opp_meta:" + opportunityId, "claimed_set:" + opportunityId)).mapEmpty();
    }

    private Opportunity insert(Opportunity o) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
            ps.setString(1, o.opportunityId());
            ps.setString(2, o.regionId());
            ps.setString(3, o.zoneId());
            ps.setLong(4, o.bookingWindowStart());
            ps.setLong(5, o.bookingWindowEnd());
            ps.setInt(6, o.capacity());
            ps.setInt(7, o.capacity());
            ps.executeUpdate();
            return new Opportunity(o.opportunityId(), o.regionId(), o.zoneId(),
                    o.capacity(), o.capacity(), o.bookingWindowStart(), o.bookingWindowEnd());
        }
    }

    private Optional<Opportunity> select(String opportunityId) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_SQL)) {
            ps.setString(1, opportunityId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(readRow(opportunityId, rs)) : Optional.empty();
            }
        }
    }

    private Optional<Opportunity> updateRow(String opportunityId, Opportunity o) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(UPDATE_SQL)) {
            ps.setString(1, o.regionId());
            ps.setString(2, o.zoneId());
            ps.setLong(3, o.bookingWindowStart());
            ps.setLong(4, o.bookingWindowEnd());
            ps.setInt(5, o.capacity());
            ps.setInt(6, o.remaining());
            ps.setString(7, opportunityId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(readRow(opportunityId, rs)) : Optional.empty();
            }
        }
    }

    private boolean deleteRow(String opportunityId) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
            ps.setString(1, opportunityId);
            return ps.executeUpdate() > 0;
        }
    }

    private static Opportunity readRow(String opportunityId, ResultSet rs) throws SQLException {
        return new Opportunity(opportunityId, rs.getString(1), rs.getString(2),
                rs.getInt(5), rs.getInt(6), rs.getLong(3), rs.getLong(4));
    }
}
