package com.tm.services.server.redis;

import io.vertx.core.Future;

/**
 * Rebuilds {@code opp_meta:{opp}} / {@code claimed_set:{opp}} in Redis from PG
 * for currently-open opportunities after a Redis restart/flush wipes its
 * in-memory dataset. PG is the source of truth (see CLAUDE.md "Redis down");
 * without this, a missing {@code opp_meta:{opp}} makes
 * {@link com.tm.common.redis.ClaimGate#claim} return {@code CLOSED} for every
 * claim on an opportunity whose booking window is, per PG, actually open.
 *
 * <p>{@link #reconcile()} is cheap (O(1)) on every call where Redis still has
 * its dataset: it checks a single heartbeat key and returns immediately. Only
 * when that key is missing (i.e. Redis lost everything) does it scan PG for
 * open opportunities and rebuild Redis state for each — so the cost doesn't
 * scale with the number of open opportunities on the common path.
 */
public interface RedisWarmupService {

    /** Restores Redis state from PG if Redis lost its dataset since the last call. */
    Future<Void> reconcile();
}
