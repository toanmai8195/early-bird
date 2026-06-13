package com.tm.common.redis;

import io.vertx.core.Future;

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
     * Used by the manager when the downstream PG settle fails entirely (nothing
     * committed), so the Redis count stays consistent with PG and Kafka replay
     * can re-admit the same drivers.
     */
    Future<Void> release(String opportunityId, String driverId);

    /**
     * Removes the driver from {@code claimed_set} AND decrements
     * {@code opp_meta.capacity} by 1. Used when PG permanently rejects a driver
     * (remaining was already 0 when the CTE ran). Unlike {@link #release}, which
     * re-opens the slot for new drivers, {@code reject} closes it permanently so
     * the gate returns FULL without letting in a replacement driver that PG would
     * reject again.
     */
    Future<Void> reject(String opportunityId, String driverId);
}
