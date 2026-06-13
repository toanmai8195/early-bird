package com.tm.services.manager;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;

/**
 * Verticle that owns one {@link ConsumerRunner} poll loop per instance.
 * Deployed with {@code setInstances(2)} so two independent consumers share the
 * same Kafka consumer group and Redis/PG infrastructure.
 *
 * <p>The poll loop runs on a plain daemon thread rather than a Vert.x worker
 * context. This is intentional: {@code handleBatch()} blocks its thread on
 * {@code Future.all(...).get()} while waiting for PG settle futures. If the
 * thread were a Vert.x WorkerContext thread, Vert.x would try to dispatch
 * Future completion callbacks back to that context — which is blocked —
 * causing a deadlock where {@code .get()} never returns.
 *
 * <p>On a plain thread (no context), Vert.x invokes Future callbacks directly
 * on the completing thread ("pg-claim-store" pool) instead of re-dispatching,
 * so the compose chain runs without needing the blocked consumer thread.
 *
 * <p>Also runs {@link PgHealthProbe} on a periodic timer (on the event loop, the
 * SELECT 1 itself on a worker) so the PG circuit breaker can recover even when the
 * server has shed all traffic and Kafka is quiet.
 */
final class ManagerVerticle extends AbstractVerticle {

    private final ConsumerRunner runner;
    private final PgHealthProbe pgHealthProbe;
    private final long probeIntervalMs;

    ManagerVerticle(ConsumerRunner runner, PgHealthProbe pgHealthProbe, long probeIntervalMs) {
        this.runner = runner;
        this.pgHealthProbe = pgHealthProbe;
        this.probeIntervalMs = probeIntervalMs;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.setPeriodic(probeIntervalMs, id -> pgHealthProbe.probe());
        startPromise.complete();
        Thread t = new Thread(runner::run, "consumer-poll-" + deploymentID());
        t.setDaemon(true);
        t.start();
    }

    @Override
    public void stop() {
        runner.shutdown();
    }
}
