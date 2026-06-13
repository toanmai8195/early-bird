package com.tm.services.manager;

import com.tm.services.manager.dao.BookingDao;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.vertx.core.Future;

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Active PG liveness probe that drives circuit-breaker recovery independent of
 * request traffic.
 *
 * <p>Without it the system can deadlock: when the breaker opens it sets the Redis
 * {@code pg_health} flag, the server stops accepting bookings, and Kafka goes
 * quiet — so the breaker would have no settle traffic to discover that PG has
 * recovered, and the flag would never clear (until its TTL). On a timer this runs
 * a cheap {@code SELECT 1} through the <em>same</em> breaker: while OPEN/HALF_OPEN a
 * successful probe spends a half-open trial and closes the breaker (which fires
 * {@code markUp} and re-opens the server); while CLOSED/DISABLED it is a no-op so
 * the healthy path costs nothing.
 */
@Singleton
public final class PgHealthProbe {

    private final CircuitBreaker breaker;
    private final BookingDao dao;

    @Inject
    public PgHealthProbe(CircuitBreaker breaker, BookingDao dao) {
        this.breaker = breaker;
        this.dao = dao;
    }

    /**
     * One probe tick. Safe to call on the event loop — the {@code SELECT 1} runs on
     * the DAO worker pool. Never fails: a probe error is recorded into the breaker
     * (which may re-open it), not propagated to the caller.
     */
    public Future<Void> probe() {
        CircuitBreaker.State state = breaker.getState();
        if (state == CircuitBreaker.State.CLOSED || state == CircuitBreaker.State.DISABLED) {
            return Future.succeededFuture();
        }
        // OPEN before the wait elapses rejects the permission (skip this tick); once
        // it elapses the breaker grants a HALF_OPEN trial, which this SELECT 1 spends
        // instead of a real booking settle.
        if (!breaker.tryAcquirePermission()) {
            return Future.succeededFuture();
        }
        long start = System.nanoTime();
        return dao.ping()
                .onSuccess(v -> breaker.onSuccess(System.nanoTime() - start, TimeUnit.NANOSECONDS))
                .onFailure(err -> breaker.onError(System.nanoTime() - start, TimeUnit.NANOSECONDS, err))
                .otherwiseEmpty();
    }
}
