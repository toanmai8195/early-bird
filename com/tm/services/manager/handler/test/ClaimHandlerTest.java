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
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
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
            new ClaimEvent("opp-1", "d1", "i1", 0L),
            new ClaimEvent("opp-1", "d2", "i2", 0L),
            new ClaimEvent("opp-1", "d3", "i3", 0L));

    @Before
    public void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerMetrics(registry);
        dao = Mockito.mock(BookingDao.class);
        gate = Mockito.mock(ClaimGate.class);
        when(gate.releaseAll(Mockito.any(), Mockito.any())).thenReturn(Future.succeededFuture());
        when(gate.rejectAll(Mockito.any(), Mockito.any())).thenReturn(Future.succeededFuture());
    }

    @Test
    public void countsEachOutcome() {
        when(dao.settleOpportunity("opp-1", batch)).thenReturn(Future.succeededFuture(List.of(
                ClaimStore.Outcome.COMMITTED,
                ClaimStore.Outcome.DUPLICATE,
                ClaimStore.Outcome.REJECTED)));

        new ClaimHandlerImpl(dao, gate, metrics, "test").handleBatch(batch);

        verify(dao, times(1)).settleOpportunity("opp-1", batch);
        assertCounter("committed", 1.0);
        assertCounter("duplicate", 1.0);
        assertCounter("rejected", 1.0);
    }

    @Test
    public void settlesEachOpportunityIndependently() {
        ClaimEvent opp1 = new ClaimEvent("opp-1", "d1", "i1", 0L);
        ClaimEvent opp2 = new ClaimEvent("opp-2", "d2", "i2", 0L);
        when(dao.settleOpportunity("opp-1", List.of(opp1)))
                .thenReturn(Future.succeededFuture(List.of(ClaimStore.Outcome.COMMITTED)));
        when(dao.settleOpportunity("opp-2", List.of(opp2)))
                .thenReturn(Future.succeededFuture(List.of(ClaimStore.Outcome.REJECTED)));

        new ClaimHandlerImpl(dao, gate, metrics, "test").handleBatch(List.of(opp1, opp2));

        verify(dao, times(1)).settleOpportunity("opp-1", List.of(opp1));
        verify(dao, times(1)).settleOpportunity("opp-2", List.of(opp2));
        assertCounter("committed", 1.0);
        assertCounter("rejected", 1.0);
    }

    @Test
    public void emptyBatchIsNoOp() {
        new ClaimHandlerImpl(dao, gate, metrics, "test").handleBatch(List.of());
        verify(dao, times(0)).settleOpportunity(Mockito.any(), Mockito.any());
    }

    @Test
    public void daoFailureCountsErrorAndRethrows() {
        when(dao.settleOpportunity("opp-1", batch))
                .thenReturn(Future.failedFuture(new java.sql.SQLException("boom")));

        ClaimHandler handler = new ClaimHandlerImpl(dao, gate, metrics, "test");
        assertThrows(HandlerException.class, () -> handler.handleBatch(batch));
        assertCounter("error", 1.0);
    }

    @Test
    public void daoFailureReleasesEachDriversRedisClaim() {
        when(dao.settleOpportunity("opp-1", batch))
                .thenReturn(Future.failedFuture(new java.sql.SQLException("boom")));

        ClaimHandler handler = new ClaimHandlerImpl(dao, gate, metrics, "test");
        assertThrows(HandlerException.class, () -> handler.handleBatch(batch));

        verify(gate, times(1)).releaseAll("opp-1", List.of("d1", "d2", "d3"));
        assertGaugeReleaseCounter("ok", 3.0);
    }

    @Test
    public void releaseFailureCountsError() {
        when(dao.settleOpportunity("opp-1", batch))
                .thenReturn(Future.failedFuture(new java.sql.SQLException("boom")));
        when(gate.releaseAll(Mockito.any(), Mockito.any()))
                .thenReturn(Future.failedFuture(new RuntimeException("redis down")));

        ClaimHandler handler = new ClaimHandlerImpl(dao, gate, metrics, "test");
        assertThrows(HandlerException.class, () -> handler.handleBatch(batch));

        assertGaugeReleaseCounter("error", 3.0);
    }

    @Test
    public void daoFailureCountsNotifyFailedForEachDriver() {
        when(dao.settleOpportunity("opp-1", batch))
                .thenReturn(Future.failedFuture(new java.sql.SQLException("boom")));

        ClaimHandler handler = new ClaimHandlerImpl(dao, gate, metrics, "test");
        assertThrows(HandlerException.class, () -> handler.handleBatch(batch));

        assertNotifyCounter("failed", 3.0);
    }

    @Test
    public void settledOutcomesCountNotifyByStatus() {
        when(dao.settleOpportunity("opp-1", batch)).thenReturn(Future.succeededFuture(List.of(
                ClaimStore.Outcome.COMMITTED,
                ClaimStore.Outcome.DUPLICATE,
                ClaimStore.Outcome.REJECTED)));

        new ClaimHandlerImpl(dao, gate, metrics, "test").handleBatch(batch);

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

        new ClaimHandlerImpl(dao, gate, metrics, "test").handleBatch(batch);

        // Only the REJECTED driver (d3) is rejected; COMMITTED (d1) keeps its slot and
        // DUPLICATE (d2, Kafka replay already committed) needs no gate action.
        verify(gate, times(0)).releaseAll(Mockito.any(), Mockito.any());
        verify(gate, times(1)).rejectAll("opp-1", List.of("d3"));
        assertGaugeReleaseCounter("ok", 1.0);
    }

    @Test
    public void largeOppIsSplitIntoSubBatchesOf10() {
        // Build 25 events for one opportunity; expect 3 settleOpportunity calls.
        List<ClaimEvent> events = new java.util.ArrayList<>();
        for (int i = 0; i < 25; i++) {
            events.add(new ClaimEvent("opp-1", "d" + i, "i" + i, 0L));
        }
        List<ClaimEvent> sub1 = events.subList(0, 10);
        List<ClaimEvent> sub2 = events.subList(10, 20);
        List<ClaimEvent> sub3 = events.subList(20, 25);

        List<ClaimStore.Outcome> committed10 = java.util.Collections.nCopies(10, ClaimStore.Outcome.COMMITTED);
        List<ClaimStore.Outcome> committed5  = java.util.Collections.nCopies(5,  ClaimStore.Outcome.COMMITTED);

        when(dao.settleOpportunity("opp-1", sub1)).thenReturn(Future.succeededFuture(committed10));
        when(dao.settleOpportunity("opp-1", sub2)).thenReturn(Future.succeededFuture(committed10));
        when(dao.settleOpportunity("opp-1", sub3)).thenReturn(Future.succeededFuture(committed5));

        new ClaimHandlerImpl(dao, gate, metrics, "test").handleBatch(events);

        verify(dao, times(1)).settleOpportunity("opp-1", sub1);
        verify(dao, times(1)).settleOpportunity("opp-1", sub2);
        verify(dao, times(1)).settleOpportunity("opp-1", sub3);
        assertCounter("committed", 25.0);
    }

    @Test
    public void subBatchFailureStopsRemainingSubBatchesForThatOpp() {
        List<ClaimEvent> events = new java.util.ArrayList<>();
        for (int i = 0; i < 15; i++) {
            events.add(new ClaimEvent("opp-1", "d" + i, "i" + i, 0L));
        }
        List<ClaimEvent> sub1 = events.subList(0, 10);
        List<ClaimEvent> sub2 = events.subList(10, 15);

        when(dao.settleOpportunity("opp-1", sub1))
                .thenReturn(Future.failedFuture(new java.sql.SQLException("boom")));

        ClaimHandler handler = new ClaimHandlerImpl(dao, gate, metrics, "test");
        assertThrows(HandlerException.class, () -> handler.handleBatch(events));

        // sub1 failed → sub2 is never attempted
        verify(dao, times(1)).settleOpportunity("opp-1", sub1);
        verify(dao, times(0)).settleOpportunity("opp-1", sub2);
        // all 10 drivers in sub1 get released in one batched call
        List<String> sub1Drivers = new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) {
            sub1Drivers.add("d" + i);
        }
        verify(gate, times(1)).releaseAll("opp-1", sub1Drivers);
    }

    @Test
    public void openCircuitFastFailsBatchWithoutTouchingPgOrRedis() {
        CircuitBreaker cb = CircuitBreaker.ofDefaults("test");
        cb.transitionToOpenState();

        ClaimHandler handler = new ClaimHandlerImpl(dao, gate, metrics, "test", cb);
        assertThrows(HandlerException.class, () -> handler.handleBatch(batch));

        // Circuit OPEN: PG never touched, Redis claim left intact, no driver notified.
        verify(dao, times(0)).settleOpportunity(Mockito.any(), Mockito.any());
        verify(gate, times(0)).releaseAll(Mockito.any(), Mockito.any());
        verify(gate, times(0)).rejectAll(Mockito.any(), Mockito.any());
        assertCounter("circuit_open", 1.0);
    }

    @Test
    public void disabledCircuitAlwaysPassesThroughToPg() {
        CircuitBreaker cb = CircuitBreaker.ofDefaults("test");
        // DISABLED never trips, never records — always passes through to PG.
        cb.transitionToDisabledState();
        when(dao.settleOpportunity("opp-1", batch)).thenReturn(Future.succeededFuture(List.of(
                ClaimStore.Outcome.COMMITTED,
                ClaimStore.Outcome.COMMITTED,
                ClaimStore.Outcome.COMMITTED)));

        new ClaimHandlerImpl(dao, gate, metrics, "test", cb).handleBatch(batch);

        verify(dao, times(1)).settleOpportunity("opp-1", batch);
        assertCounter("committed", 3.0);
    }

    @Test
    public void settleFailuresOpenTheCircuit() {
        CircuitBreaker cb = CircuitBreaker.of("test",
                io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                        .failureRateThreshold(50)
                        .slidingWindowType(io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                        .slidingWindowSize(4)
                        .minimumNumberOfCalls(4)
                        .build());
        when(dao.settleOpportunity(Mockito.eq("opp-1"), Mockito.any()))
                .thenReturn(Future.failedFuture(new java.sql.SQLException("boom")));

        ClaimHandler handler = new ClaimHandlerImpl(dao, gate, metrics, "test", cb);
        // Drive 4 single-opp batches through; all fail → failure rate 100% trips OPEN.
        for (int i = 0; i < 4; i++) {
            assertThrows(HandlerException.class,
                    () -> handler.handleBatch(List.of(new ClaimEvent("opp-1", "d", "i", 0L))));
        }
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
    }

    private void assertCounter(String result, double expected) {
        double actual = registry.get("booking.consumer.handle")
                .tag("result", result).tag("instance", "test").counter().count();
        assertEquals(expected, actual, 0.0001);
    }

    private void assertGaugeReleaseCounter(String result, double expected) {
        double actual = registry.get("booking.consumer.gate_release")
                .tag("result", result).tag("instance", "test").counter().count();
        assertEquals(expected, actual, 0.0001);
    }

    private void assertNotifyCounter(String result, double expected) {
        double actual = registry.get("booking.consumer.notify").tag("result", result).counter().count();
        assertEquals(expected, actual, 0.0001);
    }
}
