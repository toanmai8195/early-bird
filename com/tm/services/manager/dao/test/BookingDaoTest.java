package com.tm.services.manager.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.tm.common.kafka.ClaimEvent;
import com.tm.common.metric.Metrics;
import com.tm.common.metric.MicrometerMetrics;
import com.tm.common.pg.ClaimStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BookingDaoTest {

    private SimpleMeterRegistry registry;
    private Metrics metrics;
    private DataSource dataSource;
    private ClaimStore claimStore;
    private Vertx vertx;
    private WorkerExecutor workerExecutor;

    @Before
    public void setUp() throws Exception {
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerMetrics(registry);
        dataSource = Mockito.mock(DataSource.class);
        claimStore = Mockito.mock(ClaimStore.class);
        vertx = Vertx.vertx();
        workerExecutor = vertx.createSharedWorkerExecutor("test-pg-claim-store", 4);
        when(dataSource.getConnection()).thenAnswer(inv -> Mockito.mock(Connection.class));
    }

    @After
    public void tearDown() {
        workerExecutor.close();
        vertx.close();
    }

    @Test
    public void settlesOpportunityOnItsOwnConnectionAndRecordsOutcomeCounters() throws Exception {
        List<ClaimEvent> events = List.of(
                new ClaimEvent("opp-1", "d1", "i1", 0L),
                new ClaimEvent("opp-1", "d2", "i2", 0L));
        when(claimStore.settleOpportunity(Mockito.any(), Mockito.eq("opp-1"), Mockito.eq(events)))
                .thenReturn(List.of(ClaimStore.Outcome.COMMITTED, ClaimStore.Outcome.REJECTED));

        Future<List<ClaimStore.Outcome>> future =
                new JdbcBookingDao(dataSource, claimStore, metrics, workerExecutor)
                        .settleOpportunity("opp-1", events);

        List<ClaimStore.Outcome> outcomes = await(future);

        assertEquals(List.of(ClaimStore.Outcome.COMMITTED, ClaimStore.Outcome.REJECTED), outcomes);
        assertCounter("committed", 1.0);
        assertCounter("rejected", 1.0);
        assertTrue(registry.get("booking.dao.commit.latency").timer().count() >= 1);
    }

    @Test
    public void settleFailureCountsErrorAndFailsFuture() throws Exception {
        when(claimStore.settleOpportunity(Mockito.any(), Mockito.eq("opp-1"), Mockito.anyList()))
                .thenThrow(new java.sql.SQLException("boom"));

        Future<List<ClaimStore.Outcome>> future =
                new JdbcBookingDao(dataSource, claimStore, metrics, workerExecutor)
                        .settleOpportunity("opp-1", List.of(new ClaimEvent("opp-1", "d1", "i1", 0L)));

        try {
            await(future);
            org.junit.Assert.fail("expected failed future");
        } catch (java.util.concurrent.ExecutionException expected) {
            assertTrue(expected.getCause() instanceof java.sql.SQLException);
        }
        assertCounter("error", 1.0);
    }

    @Test
    public void pingRunsSelect1AndCountsOk() throws Exception {
        Statement st = Mockito.mock(Statement.class);
        Connection conn = Mockito.mock(Connection.class);
        when(conn.createStatement()).thenReturn(st);
        when(dataSource.getConnection()).thenReturn(conn);

        await(new JdbcBookingDao(dataSource, claimStore, metrics, workerExecutor).ping());

        Mockito.verify(st).execute("SELECT 1");
        assertPingCounter("ok", 1.0);
        assertTrue(registry.get("booking.dao.ping.latency").timer().count() >= 1);
    }

    @Test
    public void pingFailureCountsErrorAndFailsFuture() throws Exception {
        when(dataSource.getConnection()).thenThrow(new java.sql.SQLException("pg down"));

        Future<Void> future =
                new JdbcBookingDao(dataSource, claimStore, metrics, workerExecutor).ping();

        try {
            await(future);
            org.junit.Assert.fail("expected failed future");
        } catch (java.util.concurrent.ExecutionException expected) {
            assertTrue(expected.getCause() instanceof java.sql.SQLException);
        }
        assertPingCounter("error", 1.0);
    }

    private static <T> T await(Future<T> future) throws Exception {
        CompletableFuture<T> cf = future.toCompletionStage().toCompletableFuture();
        return cf.get();
    }

    private void assertCounter(String result, double expected) {
        double actual = registry.get("booking.dao.commit").tag("result", result).counter().count();
        assertEquals(expected, actual, 0.0001);
    }

    private void assertPingCounter(String result, double expected) {
        double actual = registry.get("booking.dao.ping").tag("result", result).counter().count();
        assertEquals(expected, actual, 0.0001);
    }
}
