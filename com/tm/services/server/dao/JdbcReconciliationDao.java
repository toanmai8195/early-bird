package com.tm.services.server.dao;

import com.tm.common.metric.Metrics;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

/**
 * JDBC {@link ReconciliationDao}.
 *
 * <p>See .claude/rules/observability-metrics.md — every DAO func records a
 * counter and a P99 latency timer.
 */
@Singleton
public final class JdbcReconciliationDao implements ReconciliationDao {

    private static final String METRIC = "booking.dao.reconciliation";
    private static final String LATENCY = "booking.dao.reconciliation.latency";

    private static final String LIST_OPEN_SQL = """
            SELECT opportunity_id, region_id, zone_id,
                   extract(epoch FROM booking_window_start)::bigint,
                   extract(epoch FROM booking_window_end)::bigint,
                   capacity, remaining
            FROM opportunities
            WHERE now() BETWEEN booking_window_start AND booking_window_end
            """;

    private static final String CLAIMED_DRIVERS_SQL =
            "SELECT driver_id FROM bookings WHERE opportunity_id = ?";

    private final DataSource dataSource;
    private final Metrics metrics;
    private final WorkerExecutor workerExecutor;

    @Inject
    public JdbcReconciliationDao(DataSource dataSource, Metrics metrics, WorkerExecutor workerExecutor) {
        this.dataSource = dataSource;
        this.metrics = metrics;
        this.workerExecutor = workerExecutor;
    }

    @Override
    public Future<List<Opportunity>> listOpenOpportunities() {
        Timer.Sample sample = Timer.start();
        return workerExecutor.executeBlocking(this::listOpen, false)
                .andThen(ar -> record(sample, "list_open", ar.succeeded()));
    }

    @Override
    public Future<List<String>> claimedDrivers(String opportunityId) {
        Timer.Sample sample = Timer.start();
        return workerExecutor.executeBlocking(() -> claimedDrivers(dataSource, opportunityId), false)
                .andThen(ar -> record(sample, "claimed_drivers", ar.succeeded()));
    }

    private void record(Timer.Sample sample, String op, boolean ok) {
        sample.stop(metrics.timer(LATENCY));
        metrics.counter(METRIC, ok ? op + "_ok" : op + "_error").increment();
    }

    private List<Opportunity> listOpen() throws SQLException {
        List<Opportunity> result = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(LIST_OPEN_SQL);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(new Opportunity(rs.getString(1), rs.getString(2), rs.getString(3),
                        rs.getInt(6), rs.getInt(7), rs.getLong(4), rs.getLong(5)));
            }
        }
        return result;
    }

    private static List<String> claimedDrivers(DataSource dataSource, String opportunityId) throws SQLException {
        List<String> drivers = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(CLAIMED_DRIVERS_SQL)) {
            ps.setString(1, opportunityId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    drivers.add(rs.getString(1));
                }
            }
        }
        return drivers;
    }
}
