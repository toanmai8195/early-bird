package com.tm.services.server;

import com.tm.services.server.config.ServerConfig;
import com.tm.services.server.di.DaggerServerComponent;
import com.tm.services.server.di.ServerComponent;

/**
 * Server entrypoint: parse config, build the Dagger object graph, deploy
 * {@link BookingVerticle}. All construction lives in
 * {@link com.tm.services.server.di.ServerModule}.
 */
public final class Main {

    public static void main(String[] args) {
        ServerConfig config = ServerConfig.load(args);
        ServerComponent component = DaggerServerComponent.factory().create(config);

        component.vertx().deployVerticle(component.bookingVerticle())
                .onSuccess(id -> System.out.printf("server (booking) deployed on port %d%n", config.port()))
                .onFailure(err -> {
                    System.err.println("deploy failed: " + err);
                    System.exit(1);
                });
    }
}
