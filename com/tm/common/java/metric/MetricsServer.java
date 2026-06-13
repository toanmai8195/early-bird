package com.tm.common.metric;

import io.vertx.core.Future;

/** Serves {@code GET /metrics} for Prometheus scraping. */
public interface MetricsServer {

    /** Binds an HTTP server on {@code port}; completes once listening. */
    Future<Void> start(int port);
}
