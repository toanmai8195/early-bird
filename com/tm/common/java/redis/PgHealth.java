package com.tm.common.redis;

import io.vertx.core.Future;

/**
 * Global PG-health kill switch shared through a single Redis key.
 *
 * <p>The manager drives it from its PG circuit breaker: when settles keep failing
 * or running slow the breaker opens and {@link #markDown()} is called; once PG
 * recovers and the breaker closes, {@link #markUp()} clears it. The server's claim
 * gate ({@link ClaimGate#claim}) reads the same key inside its Lua script and
 * fast-rejects every claim with {@link ClaimGate.Result#DOWN} while PG is down,
 * shedding load upstream of Kafka instead of piling events onto an unhealthy PG.
 */
public interface PgHealth {

    /** Redis key holding the flag: value {@value #DOWN} = unhealthy, absent = healthy. */
    String KEY = "pg_health";

    /** Value written to {@link #KEY} while PG is unhealthy. */
    String DOWN = "down";

    /**
     * TTL (seconds) on the down-flag, a fail-safe so a manager that dies while the
     * breaker is open doesn't leave the server dark forever. The breaker re-asserts
     * the flag on every transition back to OPEN (its open-state wait is shorter than
     * this TTL), so it stays fresh as long as PG is actually down.
     */
    long DOWN_TTL_SECONDS = 30;

    /** Marks PG down so the server sheds all claims. Idempotent; refreshes the TTL. */
    Future<Void> markDown();

    /** Clears the flag so the server resumes accepting claims. Idempotent. */
    Future<Void> markUp();

    /** True when PG is currently healthy (flag absent). */
    Future<Boolean> isHealthy();
}
