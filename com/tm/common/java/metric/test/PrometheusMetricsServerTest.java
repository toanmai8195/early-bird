package com.tm.common.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Vertx;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.After;
import org.junit.Test;

public class PrometheusMetricsServerTest {

    private static final int PORT = 18099;

    private final Vertx vertx = Vertx.vertx();

    @After
    public void tearDown() {
        vertx.close();
    }

    @Test
    public void scrapesRegisteredMetricsOnMetricsPath() throws Exception {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.counter("booking_test_total").increment();
        MetricsServer server = new PrometheusMetricsServer(vertx, registry);
        server.start(PORT).toCompletionStage().toCompletableFuture().get();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + PORT + "/metrics")).build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("booking_test_total"));
    }

    @Test
    public void returns404ForOtherPaths() throws Exception {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        MetricsServer server = new PrometheusMetricsServer(vertx, registry);
        server.start(PORT + 1).toCompletionStage().toCompletableFuture().get();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + (PORT + 1) + "/other")).build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(404, response.statusCode());
    }
}
