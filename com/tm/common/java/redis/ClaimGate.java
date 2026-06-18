package com.tm.common.redis;

import io.vertx.core.Future;

import java.util.List;

/**
 * Atomic early-reject gate. One Lua script per opportunity key
 * (claimed_set:{opp_id}) does counting + dedup in a single atomic op, so the
 * server can fast-reject ~90% of requests once capacity is reached.
 *
 * <p>Async (non-blocking): returns a {@link Future} so the gate can run on the
 * Vert.x event loop without blocking a worker thread.
 */
public interface ClaimGate {

    /**
     * OK = slot acquired, FULL = capacity reached, DUP = idempotent retry,
     * CLOSED = booking window not currently open, DOWN = PG unhealthy
     * (manager's circuit breaker open, see {@link PgHealth}) so the gate sheds
     * the claim before touching opportunity state.
     */
    enum Result { OK, FULL, DUP, CLOSED, DOWN }

    /**
     * Runs the atomic gate for one (opportunity, driver) pair. {@code capacity}
     * and {@code window_start} are read from {@code opp_meta:{opp}} (written once
     * when the opportunity is created, TTL'd to {@code window_end}); if that key
     * is missing (not yet open, or expired past {@code window_end}) the booking
     * window is considered closed. The gate uses Redis' own clock (atomically,
     * via {@code TIME}) so all app instances agree on whether the window is open
     * regardless of local clock skew.
     */
    Future<Result> claim(String opportunityId, String driverId);

    /**
     * Reverses a previously accepted claim, freeing the slot for other drivers.
     * Used by the server when a single claim's Kafka publish fails (the slot was
     * SADD'd but the event never made it downstream), so the Redis count stays
     * consistent.
     */
    Future<Void> release(String opportunityId, String driverId);

    /**
     * Batched {@link #release}: removes all {@code driverIds} from
     * {@code claimed_set} in a single {@code SREM}. Used by the manager when a
     * whole sub-batch's PG settle fails (nothing committed) — one round-trip
     * instead of one per driver, so Redis stays consistent with PG and Kafka
     * replay can re-admit the same drivers. No-op for an empty list.
     */
    Future<Void> releaseAll(String opportunityId, List<String> driverIds);

    /**
     * Removes the drivers from {@code claimed_set} (one {@code SREM}) AND
     * decrements {@code opp_meta.capacity} by {@code driverIds.size()} (one
     * {@code HINCRBY}). Used when PG permanently rejects drivers (remaining was
     * already 0 when the CTE ran). Unlike {@link #releaseAll}, which re-opens the
     * slots for new drivers, {@code rejectAll} closes them permanently so the gate
     * returns FULL without letting in replacement drivers that PG would reject
     * again. No-op for an empty list.
     */
    Future<Void> rejectAll(String opportunityId, List<String> driverIds);
}
