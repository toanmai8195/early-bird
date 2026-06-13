package com.tm.services.manager.handler;

import com.tm.common.exception.HandlerException;
import com.tm.common.kafka.ClaimEvent;
import com.tm.common.metric.Metrics;
import com.tm.common.pg.ClaimStore;
import com.tm.common.redis.ClaimGate;
import com.tm.services.manager.dao.BookingDao;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

/**
 * Default {@link ClaimHandler}: groups a poll batch by opportunity and settles
 * each group via {@link BookingDao} in parallel. Each opportunity's outcomes are
 * handled (counter + Redis gate release/reject + driver notify) as soon as that
 * opportunity settles, independent of how long other opportunities in the batch
 * take. {@code handleBatch} itself still waits for every opportunity before
 * returning, so the consumer only commits the Kafka offset once the whole batch
 * is durably settled (idempotent under at-least-once replay: DUPLICATE outcomes
 * are no-ops).
 *
 * <p>A {@link CircuitBreaker} wraps each PG settle: if settles keep failing or run
 * slow (&gt;5s), the circuit opens and the batch fast-fails ({@code circuit_open})
 * without touching PG or Redis. The Kafka offset isn't committed, so events redeliver
 * once the breaker half-opens — shedding load off an unhealthy PG. Enabled by default
 * (toggle via {@code --disable-circuit-breaker}).
 *
 * <p>See .claude/rules/observability-metrics.md — the consumer records a counter
 * tagged by outcome and instance.
 */
@Singleton
public final class ClaimHandlerImpl implements ClaimHandler {

    private static final String METRIC = "booking.consumer.handle";
    private static final String RELEASE_METRIC = "booking.consumer.gate_release";
    private static final String NOTIFY_METRIC = "booking.consumer.notify";
    private static final String E2E_LATENCY = "booking.e2e.notify.latency";

    private final BookingDao dao;
    private final ClaimGate gate;
    private final Metrics metrics;
    private final String instanceId;
    private final int subBatchSize;
    private final CircuitBreaker circuitBreaker;

    @Inject
    public ClaimHandlerImpl(BookingDao dao, ClaimGate gate, Metrics metrics,
                            @Named("instanceId") String instanceId,
                            @Named("settleBatchSize") int subBatchSize,
                            CircuitBreaker circuitBreaker) {
        this.dao = dao;
        this.gate = gate;
        this.metrics = metrics;
        this.instanceId = instanceId;
        this.subBatchSize = subBatchSize;
        this.circuitBreaker = circuitBreaker;
    }

    /** Test-only convenience: defaults the settle sub-batch size to 10. */
    ClaimHandlerImpl(BookingDao dao, ClaimGate gate, Metrics metrics, String instanceId) {
        this(dao, gate, metrics, instanceId, CircuitBreaker.ofDefaults("pg-settle"));
    }

    /** Test-only convenience: default sub-batch size with an explicit breaker. */
    ClaimHandlerImpl(BookingDao dao, ClaimGate gate, Metrics metrics, String instanceId,
                     CircuitBreaker circuitBreaker) {
        this(dao, gate, metrics, instanceId, 10, circuitBreaker);
    }

    @Override
    public void handleBatch(List<ClaimEvent> events) {
        if (events.isEmpty()) {
            return;
        }

        // Group by opportunity, preserving arrival order within each group.
        Map<String, List<ClaimEvent>> byOpp = new LinkedHashMap<>();
        for (ClaimEvent e : events) {
            byOpp.computeIfAbsent(e.opportunityId(), k -> new ArrayList<>()).add(e);
        }

        // Each opp's events are settled in sequential sub-batches of subBatchSize.
        // Different opps run in parallel. Sequential within an opp avoids concurrent
        // row-lock contention on the same opportunities row. For a single hot opp the
        // settle serializes on that row regardless, so a larger subBatchSize cuts the
        // number of round-trips (main lever for the contended pattern).
        List<Future<?>> futures = new ArrayList<>(byOpp.size());
        for (Map.Entry<String, List<ClaimEvent>> entry : byOpp.entrySet()) {
            String oppId = entry.getKey();
            List<List<ClaimEvent>> subBatches = partition(entry.getValue(), subBatchSize);

            Future<Void> chain = Future.succeededFuture();
            for (List<ClaimEvent> sub : subBatches) {
                chain = chain.compose(v ->
                        settleViaBreaker(oppId, sub)
                                .onSuccess(outcomes -> onSettled(sub, outcomes))
                                .onFailure(err -> onSettleFailed(oppId, sub, err))
                                .mapEmpty());
            }
            futures.add(chain);
        }

        try {
            Future.all(futures).toCompletionStage().toCompletableFuture().get();
        } catch (ExecutionException ex) {
            throw new HandlerException("failed to settle claim batch", ex.getCause());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new HandlerException("interrupted while settling claim batch", ex);
        }
    }

    /**
     * Runs one PG settle through the circuit breaker. When the circuit is OPEN the
     * call is fast-failed with {@link CallNotPermittedException} (no PG round-trip);
     * otherwise the settle's success/failure and elapsed time feed the breaker so a
     * run of errors or slow (&gt;5s) settles trips it OPEN.
     */
    private Future<List<ClaimStore.Outcome>> settleViaBreaker(String oppId, List<ClaimEvent> sub) {
        if (!circuitBreaker.tryAcquirePermission()) {
            return Future.failedFuture(CallNotPermittedException.createCallNotPermittedException(circuitBreaker));
        }
        long start = System.nanoTime();
        return dao.settleOpportunity(oppId, sub)
                .andThen(ar -> {
                    long elapsed = System.nanoTime() - start;
                    if (ar.succeeded()) {
                        circuitBreaker.onSuccess(elapsed, TimeUnit.NANOSECONDS);
                    } else {
                        circuitBreaker.onError(elapsed, TimeUnit.NANOSECONDS, ar.cause());
                    }
                });
    }

    private static <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> result = new ArrayList<>((list.size() + size - 1) / size);
        for (int i = 0; i < list.size(); i += size) {
            result.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return result;
    }

    /**
     * The PG write failed for this opportunity's whole batch (nothing committed):
     * release each driver's Redis claim so the slot is free again and Redis stays
     * consistent with PG, and tell each driver's app the claim failed. The batch
     * itself is retried via Kafka redelivery (offset not committed); PG's UNIQUE
     * constraint makes that retry idempotent, so a driver may see a later
     * "confirmed" notification superseding this "failed" one.
     */
    private void onSettleFailed(String opportunityId, List<ClaimEvent> events, Throwable cause) {
        if (cause instanceof CallNotPermittedException) {
            // Circuit OPEN: PG is unhealthy (errors or >5s settles). Nothing was
            // attempted against PG, so don't release the Redis claim or notify drivers —
            // just count it and let the batch fail so the offset isn't committed and
            // Kafka redelivers once the breaker half-opens.
            metrics.counter(METRIC, "circuit_open", instanceId).increment();
            return;
        }
        metrics.counter(METRIC, "error", instanceId).increment();
        for (ClaimEvent e : events) {
            gate.release(opportunityId, e.driverId())
                    .onSuccess(v -> metrics.counter(RELEASE_METRIC, "ok", instanceId).increment())
                    .onFailure(err -> metrics.counter(RELEASE_METRIC, "error", instanceId).increment());
            notify("failed", e.serverReceivedAt());
        }
    }

    /**
     * Runs as soon as this opportunity settles, independent of the rest of the batch.
     * REJECTED: driver passed the Redis gate (SADD) but PG had remaining=0 — call
     * reject() to close the slot permanently (SREM + capacity-1) so the gate returns
     * FULL without admitting a replacement driver that PG would reject again.
     * DUPLICATE: Kafka replay of an already-committed booking — driver is still in
     * claimed_set from the original OK, SCARD is correct, no Redis action needed.
     */
    private void onSettled(List<ClaimEvent> events, List<ClaimStore.Outcome> outcomes) {
        for (int i = 0; i < events.size(); i++) {
            ClaimEvent e = events.get(i);
            ClaimStore.Outcome o = outcomes.get(i);
            metrics.counter(METRIC, o.name().toLowerCase(), instanceId).increment();

            String status;
            switch (o) {
                case COMMITTED -> status = "confirmed";
                case DUPLICATE -> status = "duplicate";
                default -> status = "failed";
            }
            notify(status, e.serverReceivedAt());

            if (o == ClaimStore.Outcome.REJECTED) {
                gate.reject(e.opportunityId(), e.driverId())
                        .onSuccess(v -> metrics.counter(RELEASE_METRIC, "ok", instanceId).increment())
                        .onFailure(err -> metrics.counter(RELEASE_METRIC, "error", instanceId).increment());
            }
        }
    }

    /** TODO: push to driver app (MQTT/WS). Records e2e latency when serverReceivedAt > 0. */
    private void notify(String status, long serverReceivedAt) {
        metrics.counter(NOTIFY_METRIC, status).increment();
        if (serverReceivedAt > 0) {
            long elapsedMs = System.currentTimeMillis() - serverReceivedAt;
            metrics.timer(E2E_LATENCY, status).record(elapsedMs, TimeUnit.MILLISECONDS);
        }
    }
}
