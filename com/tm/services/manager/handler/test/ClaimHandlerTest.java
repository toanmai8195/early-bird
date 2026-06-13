package com.tm.services.manager.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tm.common.exception.HandlerException;
import com.tm.common.kafka.ClaimEvent;
import com.tm.common.metric.Metrics;
import com.tm.common.metric.MicrometerMetrics;
import com.tm.common.pg.ClaimStore;
import com.tm.common.redis.ClaimGate;
import com.tm.services.manager.dao.BookingDao;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ClaimHandlerTest {

    private SimpleMeterRegistry registry;
    private Metrics metrics;
    private BookingDao dao;
    private ClaimGate gate;
    private final List<ClaimEvent> batch = List.of(
            new ClaimEvent("opp-1", "d1", "i1"),
            new ClaimEvent("opp-1", "d2", "i2"),
            new ClaimEvent("opp-1", "d3", "i3"));

    @Before
    public void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerMetrics(registry);
        dao = Mockito.mock(BookingDao.class);
        gate = Mockito.mock(ClaimGate.class);
        when(gate.release(Mockito.any(), Mockito.any())).thenReturn(Future.succeededFuture());
        when(gate.reject(Mockito.any(), Mockito.any())).thenReturn(Future.succeededFuture());
    }

    @Test
    public void countsEachOutcome() {
        when(dao.settleOpportunity("opp-1", batch)).thenReturn(Future.succeededFuture(List.of(
                ClaimStore.Outcome.COMMITTED,
                ClaimStore.Outcome.DUPLICATE,
                ClaimStore.Outcome.REJECTED)));

        new ClaimHandlerImpl(dao, gate, metrics).handleBatch(batch);

        verify(dao, times(1)).settleOpportunity("opp-1", batch);
        assertCounter("committed", 1.0);
        assertCounter("duplicate", 1.0);
        assertCounter("rejected", 1.0);
    }

    @Test
    public void settlesEachOpportunityIndependently() {
        ClaimEvent opp1 = new ClaimEvent("opp-1", "d1", "i1");
        ClaimEvent opp2 = new ClaimEvent("opp-2", "d2", "i2");
        when(dao.settleOpportunity("opp-1", List.of(opp1)))
                .thenReturn(Future.succeededFuture(List.of(ClaimStore.Outcome.COMMITTED)));
        when(dao.settleOpportunity("opp-2", List.of(opp2)))
                .thenReturn(Future.succeededFuture(List.of(ClaimStore.Outcome.REJECTED)));

        new ClaimHandlerImpl(dao, gate, metrics).handleBatch(List.of(opp1, opp2));

        verify(dao, times(1)).settleOpportunity("opp-1", List.of(opp1));
        verify(dao, times(1)).settleOpportunity("opp-2", List.of(opp2));
        assertCounter("committed", 1.0);
        assertCounter("rejected", 1.0);
    }

    @Test
    public void emptyBatchIsNoOp() {
        new ClaimHandlerImpl(dao, gate, metrics).handleBatch(List.of());
        verify(dao, times(0)).settleOpportunity(Mockito.any(), Mockito.any());
    }

    @Test
    public void daoFailureCountsErrorAndRethrows() {
        when(dao.settleOpportunity("opp-1", batch))
                .thenReturn(Future.failedFuture(new java.sql.SQLException("boom")));

        ClaimHandler handler = new ClaimHandlerImpl(dao, gate, metrics);
        assertThrows(HandlerException.class, () -> handler.handleBatch(batch));
        assertCounter("error", 1.0);
    }

    @Test
    public void daoFailureReleasesEachDriversRedisClaim() {
        when(dao.settleOpportunity("opp-1", batch))
                .thenReturn(Future.failedFuture(new java.sql.SQLException("boom")));

        ClaimHandler handler = new ClaimHandlerImpl(dao, gate, metrics);
        assertThrows(HandlerException.class, () -> handler.handleBatch(batch));

        verify(gate, times(1)).release("opp-1", "d1");
        verify(gate, times(1)).release("opp-1", "d2");
        verify(gate, times(1)).release("opp-1", "d3");
        assertGaugeReleaseCounter("ok", 3.0);
    }

    @Test
    public void releaseFailureCountsError() {
        when(dao.settleOpportunity("opp-1", batch))
                .thenReturn(Future.failedFuture(new java.sql.SQLException("boom")));
        when(gate.release(Mockito.any(), Mockito.any()))
                .thenReturn(Future.failedFuture(new RuntimeException("redis down")));

        ClaimHandler handler = new ClaimHandlerImpl(dao, gate, metrics);
        assertThrows(HandlerException.class, () -> handler.handleBatch(batch));

        assertGaugeReleaseCounter("error", 3.0);
    }

    @Test
    public void daoFailureCountsNotifyFailedForEachDriver() {
        when(dao.settleOpportunity("opp-1", batch))
                .thenReturn(Future.failedFuture(new java.sql.SQLException("boom")));

        ClaimHandler handler = new ClaimHandlerImpl(dao, gate, metrics);
        assertThrows(HandlerException.class, () -> handler.handleBatch(batch));

        assertNotifyCounter("failed", 3.0);
    }

    @Test
    public void settledOutcomesCountNotifyByStatus() {
        when(dao.settleOpportunity("opp-1", batch)).thenReturn(Future.succeededFuture(List.of(
                ClaimStore.Outcome.COMMITTED,
                ClaimStore.Outcome.DUPLICATE,
                ClaimStore.Outcome.REJECTED)));

        new ClaimHandlerImpl(dao, gate, metrics).handleBatch(batch);

        assertNotifyCounter("confirmed", 1.0);
        assertNotifyCounter("duplicate", 1.0);
        assertNotifyCounter("failed", 1.0);
    }

    @Test
    public void committedLeavesRedisSlotIntact() {
        when(dao.settleOpportunity("opp-1", batch)).thenReturn(Future.succeededFuture(List.of(
                ClaimStore.Outcome.COMMITTED,
                ClaimStore.Outcome.DUPLICATE,
                ClaimStore.Outcome.REJECTED)));

        new ClaimHandlerImpl(dao, gate, metrics).handleBatch(batch);

        // COMMITTED: slot stays (driver permanently holds it)
        verify(gate, times(0)).release("opp-1", "d1");
        verify(gate, times(0)).reject("opp-1", "d1");
        // DUPLICATE: Kafka replay, already committed — no gate action
        verify(gate, times(0)).release("opp-1", "d2");
        verify(gate, times(0)).reject("opp-1", "d2");
        // REJECTED: remove from set AND decrement capacity so gate returns FULL
        verify(gate, times(0)).release("opp-1", "d3");
        verify(gate, times(1)).reject("opp-1", "d3");
        assertGaugeReleaseCounter("ok", 1.0);
    }

    private void assertCounter(String result, double expected) {
        double actual = registry.get("booking.consumer.handle").tag("result", result).counter().count();
        assertEquals(expected, actual, 0.0001);
    }

    private void assertGaugeReleaseCounter(String result, double expected) {
        double actual = registry.get("booking.consumer.gate_release").tag("result", result).counter().count();
        assertEquals(expected, actual, 0.0001);
    }

    private void assertNotifyCounter(String result, double expected) {
        double actual = registry.get("booking.consumer.notify").tag("result", result).counter().count();
        assertEquals(expected, actual, 0.0001);
    }
}
