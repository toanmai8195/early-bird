package com.tm.services.server.handler;

import com.tm.common.kafka.ClaimEvent;
import com.tm.common.kafka.ClaimProducer;
import com.tm.common.metric.Metrics;
import com.tm.common.redis.ClaimGate;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;

import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Handles {@code POST /opportunities/{id}/bookings}, fully non-blocking on the
 * Vert.x event loop: the Redis gate ({@link ClaimGate}) fast-rejects ~90% of
 * requests when an opportunity is full, accepted claims are published to Kafka
 * ({@link ClaimProducer}), and the driver gets 202 ACCEPTED (not a synchronous
 * 200). No worker-thread blocking.
 *
 * <p>A {@link CircuitBreaker} wraps the gate call: if Redis is down/timing out,
 * the circuit opens and all requests are rejected with 503 (no degraded-pass-through).
 * PG's atomic decrement + UNIQUE remain the correctness backstop, but without the
 * Redis gate the fairness and fast-reject guarantees are lost, so we prefer to shed
 * load rather than silently degrade (see CLAUDE.md "Redis down").
 *
 * <p>The gate also carries a global PG-health kill switch: when the manager's PG
 * circuit breaker opens it sets a Redis flag (see {@code PgHealth}), and the gate
 * returns {@code DOWN} so this endpoint sheds claims with 503 until PG recovers and
 * the manager clears the flag — keeping load off an unhealthy PG upstream of Kafka.
 *
 * <p>See .claude/rules/observability-metrics.md — the endpoint records a counter
 * tagged by outcome + a P99 latency timer.
 */
@Singleton
public final class ClaimHandlerImpl implements ClaimHandler {

    private static final String METRIC = "booking.api.claim";
    private static final String LATENCY = "booking.api.claim.latency";

    private final ClaimGate gate;
    private final ClaimProducer producer;
    private final Metrics metrics;
    private final CircuitBreaker circuitBreaker;

    @Inject
    public ClaimHandlerImpl(ClaimGate gate, ClaimProducer producer, Metrics metrics,
                             CircuitBreaker circuitBreaker) {
        this.gate = gate;
        this.producer = producer;
        this.metrics = metrics;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public void handle(RoutingContext ctx) {
        Timer.Sample sample = Timer.start();
        String opportunityId = ctx.pathParam("id");
        String driverId = ctx.request().getHeader("X-Driver-Id");
        String idempotencyKey = ctx.request().getHeader("X-Idempotency-Key");

        if (opportunityId == null || opportunityId.isBlank()
                || driverId == null || driverId.isBlank()
                || idempotencyKey == null || idempotencyKey.isBlank()) {
            metrics.counter(METRIC, "bad_request").increment();
            sample.stop(metrics.timer(LATENCY));
            ctx.response()
                    .setStatusCode(400)
                    .putHeader("content-type", "application/json")
                    .end("{\"status\":\"BAD_REQUEST\"}");
            return;
        }

        claimViaGate(opportunityId, driverId)
                .compose(outcome -> switch (outcome) {
                    case OK -> producer.publish(ClaimEvent.now(opportunityId, driverId, idempotencyKey))
                            .map(v -> new Reply(202, "ACCEPTED", "ok"))
                            .recover(err -> {
                                System.err.printf("publish failed opp=%s driver=%s: %s: %s%n",
                                        opportunityId, driverId,
                                        err.getClass().getSimpleName(), err.getMessage());
                                return gate.release(opportunityId, driverId)
                                        .map(v -> new Reply(503, "UNAVAILABLE", "error"));
                            });
                    case DUP -> Future.succeededFuture(new Reply(200, "ACCEPTED", "dup"));
                    case FULL -> Future.succeededFuture(new Reply(409, "FULL", "full"));
                    case CLOSED -> Future.succeededFuture(new Reply(409, "CLOSED", "closed"));
                    case DOWN -> Future.succeededFuture(new Reply(503, "PG_UNAVAILABLE", "down"));
                    case THROTTLED -> Future.succeededFuture(new Reply(503, "THROTTLED", "throttled"));
                })
                .onComplete(ar -> {
                    if (ar.failed()) {
                        System.err.printf("claim pipeline failed opp=%s driver=%s: %s: %s%n",
                                opportunityId, driverId,
                                ar.cause().getClass().getSimpleName(), ar.cause().getMessage());
                    }
                    Reply reply = ar.succeeded() ? ar.result() : new Reply(503, "UNAVAILABLE", "error");
                    metrics.counter(METRIC, reply.metric()).increment();
                    sample.stop(metrics.timer(LATENCY, reply.metric()));
                    ctx.response()
                            .setStatusCode(reply.status())
                            .putHeader("content-type", "application/json")
                            .end("{\"status\":\"" + reply.body() + "\"}");
                });
    }

    /**
     * Runs the Redis gate through the circuit breaker. If Redis is down (circuit
     * OPEN), skips the gate entirely and falls back to {@link #degraded()}.
     */
    private Future<Outcome> claimViaGate(String opportunityId, String driverId) {
        if (!circuitBreaker.tryAcquirePermission()) {
            return degraded();
        }

        long start = System.nanoTime();
        return gate.claim(opportunityId, driverId)
                .andThen(ar -> {
                    long elapsed = System.nanoTime() - start;
                    if (ar.succeeded()) {
                        circuitBreaker.onSuccess(elapsed, TimeUnit.NANOSECONDS);
                    } else {
                        circuitBreaker.onError(elapsed, TimeUnit.NANOSECONDS, ar.cause());
                    }
                })
                .map(Outcome::from)
                .recover(err -> err instanceof CallNotPermittedException
                        ? degraded()
                        : Future.failedFuture(err));
    }

    private Future<Outcome> degraded() {
        return Future.succeededFuture(Outcome.THROTTLED);
    }

    private enum Outcome {
        OK, FULL, DUP, CLOSED, DOWN, THROTTLED;

        static Outcome from(ClaimGate.Result result) {
            return switch (result) {
                case OK -> OK;
                case FULL -> FULL;
                case DUP -> DUP;
                case CLOSED -> CLOSED;
                case DOWN -> DOWN;
            };
        }
    }

    private record Reply(int status, String body, String metric) {}
}
