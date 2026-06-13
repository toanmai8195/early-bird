package com.tm.services.server;

import com.tm.common.metric.MetricsServer;
import com.tm.services.server.config.ServerConfig;
import com.tm.services.server.handler.ClaimHandler;
import com.tm.services.server.handler.OpportunityHandler;
import com.tm.services.server.redis.RedisWarmupService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import javax.inject.Inject;

/**
 * HTTP server entrypoint: wires routes to handlers and listens on
 * {@link ServerConfig#port()}. Business logic lives in {@link ClaimHandler}
 * (claim endpoint) and {@link OpportunityHandler} (opportunity CRUD). Also
 * runs {@link RedisWarmupService} periodically to self-heal Redis state lost
 * to a restart, and starts {@link MetricsServer} for Prometheus scraping.
 */
public class BookingVerticle extends AbstractVerticle {

    private final ClaimHandler claimHandler;
    private final OpportunityHandler opportunityHandler;
    private final RedisWarmupService redisWarmupService;
    private final MetricsServer metricsServer;
    private final ServerConfig config;

    @Inject
    public BookingVerticle(ClaimHandler claimHandler, OpportunityHandler opportunityHandler,
                            RedisWarmupService redisWarmupService, MetricsServer metricsServer,
                            ServerConfig config) {
        this.claimHandler = claimHandler;
        this.opportunityHandler = opportunityHandler;
        this.redisWarmupService = redisWarmupService;
        this.metricsServer = metricsServer;
        this.config = config;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.setPeriodic(config.redisWarmupIntervalMs(), id -> redisWarmupService.reconcile());
        metricsServer.start(config.metricsPort());

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.post("/opportunities/:id/bookings").handler(claimHandler);
        router.post("/opportunities/:id").handler(opportunityHandler::create);
        router.get("/opportunities/:id").handler(opportunityHandler::get);
        router.put("/opportunities/:id").handler(opportunityHandler::update);
        router.delete("/opportunities/:id").handler(opportunityHandler::delete);
        router.get("/health").handler(ctx -> ctx.json(java.util.Map.of("status", "ok")));

        vertx.createHttpServer()
                .requestHandler(router)
                .listen(config.port())
                .<Void>mapEmpty()
                .onComplete(startPromise);
    }
}
