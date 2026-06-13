package com.tm.services.manager;

import com.tm.common.metric.PrometheusMetricsServer;
import com.tm.services.manager.config.ManagerConfig;
import com.tm.services.manager.di.DaggerManagerComponent;
import com.tm.services.manager.di.ManagerComponent;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;

import java.util.concurrent.atomic.AtomicInteger;

public final class Main {

    public static void main(String[] args) throws InterruptedException {
        ManagerConfig config = ManagerConfig.load(args);

        Vertx vertx = Vertx.vertx();
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        new PrometheusMetricsServer(vertx, registry).start(config.metricsPort());

        AtomicInteger seq = new AtomicInteger();
        vertx.deployVerticle(
                () -> {
                    String instanceId = "manager-" + seq.getAndIncrement();
                    ManagerComponent c = DaggerManagerComponent.factory()
                            .create(config, registry, vertx, instanceId);
                    return new ManagerVerticle(
                            c.consumerRunner(), c.pgHealthProbe(), config.pgProbeIntervalMs());
                },
                new DeploymentOptions().setInstances(2)
        ).toCompletionStage().toCompletableFuture().join();

        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                vertx.close().toCompletionStage().toCompletableFuture().join()));

        Thread.currentThread().join();
    }
}
