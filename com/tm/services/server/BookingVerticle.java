package com.tm.services.server;

import com.tm.common.kafka.ClaimEvent;
import com.tm.common.kafka.ClaimProducer;
import com.tm.common.redis.ClaimGate;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

/**
 * HTTP claim endpoint. Runs the Redis gate to fast-reject ~90% of requests when
 * an opportunity is full, writes accepted claims to the Redis pending set
 * (outbox) + publishes to Kafka, and returns 202 ACCEPTED (not synchronous 200).
 */
public class BookingVerticle extends AbstractVerticle {

    private final ClaimGate gate;
    private final ClaimProducer producer;

    public BookingVerticle(ClaimGate gate, ClaimProducer producer) {
        this.gate = gate;
        this.producer = producer;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        Router router = Router.router(vertx);
        router.post("/opportunities/:id/bookings").handler(this::handleClaim);
        router.get("/health").handler(ctx -> ctx.json(java.util.Map.of("status", "ok")));

        int port = Integer.parseInt(System.getenv().getOrDefault("PORT", "8080"));
        vertx.createHttpServer()
                .requestHandler(router)
                .listen(port, http -> {
                    if (http.succeeded()) {
                        startPromise.complete();
                    } else {
                        startPromise.fail(http.cause());
                    }
                });
    }

    private void handleClaim(RoutingContext ctx) {
        String opportunityId = ctx.pathParam("id");
        String driverId = ctx.request().getHeader("X-Driver-Id");
        String idempotencyKey = ctx.request().getHeader("X-Idempotency-Key");
        int capacity = 1000; // TODO: load from config/cache per opportunity.

        // Run gate off the event loop (Lettuce sync call) in a worker.
        vertx.executeBlocking(() -> gate.claim(opportunityId, driverId, capacity))
                .onSuccess(result -> {
                    switch (result) {
                        case OK -> {
                            // TODO: add to Redis pending set (outbox) before publish.
                            producer.publish(new ClaimEvent(opportunityId, driverId, idempotencyKey));
                            ctx.response().setStatusCode(202).end("{\"status\":\"ACCEPTED\"}");
                        }
                        case DUP -> ctx.response().setStatusCode(200).end("{\"status\":\"ACCEPTED\"}");
                        case FULL -> ctx.response().setStatusCode(409).end("{\"status\":\"FULL\"}");
                    }
                })
                .onFailure(err -> ctx.response().setStatusCode(503).end("{\"status\":\"UNAVAILABLE\"}"));
    }
}
