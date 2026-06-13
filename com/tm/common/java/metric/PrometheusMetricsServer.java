package com.tm.common.metric;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import javax.inject.Inject;
import javax.inject.Singleton;

/** Vert.x-backed {@link MetricsServer} that scrapes a {@link PrometheusMeterRegistry}. */
@Singleton
public final class PrometheusMetricsServer implements MetricsServer {

    private final Vertx vertx;
    private final PrometheusMeterRegistry registry;

    @Inject
    public PrometheusMetricsServer(Vertx vertx, PrometheusMeterRegistry registry) {
        this.vertx = vertx;
        this.registry = registry;
    }

    @Override
    public Future<Void> start(int port) {
        return vertx.createHttpServer()
                .requestHandler(req -> {
                    if ("/metrics".equals(req.path())) {
                        req.response()
                                .putHeader("Content-Type", "text/plain; version=0.0.4")
                                .end(registry.scrape());
                    } else {
                        req.response().setStatusCode(404).end();
                    }
                })
                .listen(port)
                .mapEmpty();
    }
}
