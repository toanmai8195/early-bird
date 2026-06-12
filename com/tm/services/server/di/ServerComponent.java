package com.tm.services.server.di;

import com.tm.services.server.BookingVerticle;
import com.tm.services.server.config.ServerConfig;
import dagger.BindsInstance;
import dagger.Component;
import io.vertx.core.Vertx;

import javax.inject.Singleton;

/** Root Dagger component; {@link ServerConfig} is supplied at creation time. */
@Singleton
@Component(modules = {ServerModule.class, ServerBindings.class})
public interface ServerComponent {

    Vertx vertx();

    BookingVerticle bookingVerticle();

    @Component.Factory
    interface Factory {
        ServerComponent create(@BindsInstance ServerConfig config);
    }
}
