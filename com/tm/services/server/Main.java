package com.tm.services.server;

import io.vertx.core.Vertx;

/**
 * Entrypoint drivers hit to claim opportunities. Uses Redis to reject early and
 * Kafka to hand off accepted claims to the manager.
 */
public final class Main {

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        // TODO: build ClaimGate (Lettuce) + ClaimProducer (Kafka) from config/env.
        vertx.deployVerticle(new BookingVerticle(null, null), res -> {
            if (res.succeeded()) {
                System.out.println("server (booking) deployed");
            } else {
                System.err.println("deploy failed: " + res.cause());
                System.exit(1);
            }
        });
    }
}
