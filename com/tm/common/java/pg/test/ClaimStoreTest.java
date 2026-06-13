package com.tm.common.pg;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tm.common.kafka.ClaimEvent;
import com.tm.common.metric.Metrics;
import com.tm.common.metric.MicrometerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ClaimStoreTest {

    private Connection conn;
    private PreparedStatement ps;
    private ResultSet rs;
    private SimpleMeterRegistry registry;
    private PgClaimStore store;

    @Before
    public void setUp() throws Exception {
        conn = Mockito.mock(Connection.class);
        ps = Mockito.mock(PreparedStatement.class);
        rs = Mockito.mock(ResultSet.class);
        when(conn.prepareStatement(Mockito.anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        registry = new SimpleMeterRegistry();
        store = new PgClaimStore(new MicrometerMetrics(registry));
    }

    @Test
    public void mapsPerDriverOutcomeToEventsWithOneBulkStatement() throws Exception {
        // one opportunity -> one bulk statement; d1 appears twice (intra-batch dup)
        List<ClaimEvent> events = List.of(
                new ClaimEvent("opp-1", "d1", "i1", 0L),
                new ClaimEvent("opp-1", "d2", "i2", 0L),
                new ClaimEvent("opp-1", "d1", "i1", 0L));
        // SQL returns one row per distinct driver: (driver, outcome)
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString(1)).thenReturn("d1", "d2");
        when(rs.getString(2)).thenReturn("COMMITTED", "REJECTED");

        List<ClaimStore.Outcome> outcomes = store.settleOpportunity(conn, "opp-1", events);

        // both d1 events -> COMMITTED, d2 -> REJECTED, in input order
        assertEquals(List.of(
                ClaimStore.Outcome.COMMITTED,
                ClaimStore.Outcome.REJECTED,
                ClaimStore.Outcome.COMMITTED), outcomes);
        verify(ps, times(1)).executeQuery(); // one bulk statement for the opp

        // 2× COMMITTED + 1× REJECTED counted individually per event (per-driver)
        assertEquals(2.0, counter("committed"), 0.0001);
        assertEquals(1.0, counter("rejected"),  0.0001);
        assertEquals(0.0, counter("error"),     0.0001);
        // one settleOpportunity() call = one batch increment (per-batch)
        assertEquals(1.0, batchCounter("ok"),    0.0001);
        assertEquals(0.0, batchCounter("error"), 0.0001);
    }

    @Test(expected = java.sql.SQLException.class)
    public void propagatesSqlExceptionAndCountsError() throws Exception {
        when(ps.executeQuery()).thenThrow(new java.sql.SQLException("boom"));
        try {
            store.settleOpportunity(conn, "opp-1", List.of(new ClaimEvent("opp-1", "d1", "i1", 0L)));
        } catch (java.sql.SQLException e) {
            assertEquals(1.0, counter("error"), 0.0001);
            assertEquals(1.0, batchCounter("error"), 0.0001);
            throw e;
        }
    }

    private double counter(String result) {
        return registry.find("booking.pg.settle").tag("result", result).counters().stream()
                .mapToDouble(c -> c.count()).sum();
    }

    private double batchCounter(String result) {
        return registry.find("booking.pg.settle.batch").tag("result", result).counters().stream()
                .mapToDouble(c -> c.count()).sum();
    }
}
