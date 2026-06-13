package com.tm.services.manager.handler;

import com.tm.common.exception.HandlerException;
import com.tm.common.kafka.ClaimEvent;
import com.tm.common.metric.Metrics;
import com.tm.common.pg.ClaimStore;
import com.tm.common.redis.ClaimGate;
import com.tm.services.manager.dao.BookingDao;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Default {@link ClaimHandler}: groups a poll batch by opportunity and settles
 * each group via {@link BookingDao} in parallel. Each opportunity's outcomes are
 * handled (counter + Redis pending-set removal + driver notify) as soon as that
 * opportunity settles, independent of how long other opportunities in the batch
 * take. {@code handleBatch} itself still waits for every opportunity before
 * returning, so the consumer only commits the Kafka offset once the whole batch
 * is durably settled (idempotent under at-least-once replay: DUPLICATE outcomes
 * are no-ops).
 *
 * <p>See .claude/rules/observability-metrics.md — the consumer records a counter
 * tagged by outcome.
 */
@Singleton
public final class ClaimHandlerImpl implements ClaimHandler {

    private static final String METRIC = "booking.consumer.handle";
    private static final String RELEASE_METRIC = "booking.consumer.gate_release";
    private static final String NOTIFY_METRIC = "booking.consumer.notify";

    private final BookingDao dao;
    private final ClaimGate gate;
    private final Metrics metrics;

    @Inject
    public ClaimHandlerImpl(BookingDao dao, ClaimGate gate, Metrics metrics) {
        this.dao = dao;
        this.gate = gate;
        this.metrics = metrics;
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

        List<Future<List<ClaimStore.Outcome>>> futures = new ArrayList<>(byOpp.size());
        for (Map.Entry<String, List<ClaimEvent>> entry : byOpp.entrySet()) {
            List<ClaimEvent> oppEvents = entry.getValue();
            futures.add(dao.settleOpportunity(entry.getKey(), oppEvents)
                    .onSuccess(outcomes -> onSettled(oppEvents, outcomes))
                    .onFailure(err -> onSettleFailed(entry.getKey(), oppEvents)));
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
     * The PG write failed for this opportunity's whole batch (nothing committed):
     * release each driver's Redis claim so the slot is free again and Redis stays
     * consistent with PG, and tell each driver's app the claim failed. The batch
     * itself is retried via Kafka redelivery (offset not committed); PG's UNIQUE
     * constraint makes that retry idempotent, so a driver may see a later
     * "confirmed" notification superseding this "failed" one.
     */
    private void onSettleFailed(String opportunityId, List<ClaimEvent> events) {
        metrics.counter(METRIC, "error").increment();
        for (ClaimEvent e : events) {
            gate.release(opportunityId, e.driverId())
                    .onSuccess(v -> metrics.counter(RELEASE_METRIC, "ok").increment())
                    .onFailure(err -> metrics.counter(RELEASE_METRIC, "error").increment());
            notify("failed");
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
            metrics.counter(METRIC, o.name().toLowerCase()).increment();

            String status;
            switch (o) {
                case COMMITTED -> status = "confirmed";
                case DUPLICATE -> status = "duplicate";
                default -> status = "failed";
            }
            notify(status);

            if (o == ClaimStore.Outcome.REJECTED) {
                gate.reject(e.opportunityId(), e.driverId())
                        .onSuccess(v -> metrics.counter(RELEASE_METRIC, "ok").increment())
                        .onFailure(err -> metrics.counter(RELEASE_METRIC, "error").increment());
            }
        }
    }

    /** TODO: push to driver app (MQTT/WS). Tracked via its own counter for now. */
    private void notify(String status) {
        metrics.counter(NOTIFY_METRIC, status).increment();
    }
}
