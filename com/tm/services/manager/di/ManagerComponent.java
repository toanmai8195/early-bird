package com.tm.services.manager.di;

import com.tm.services.manager.ConsumerRunner;
import com.tm.services.manager.PgHealthProbe;
import com.tm.services.manager.config.ManagerConfig;
import dagger.BindsInstance;
import dagger.Component;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Vertx;

import javax.inject.Named;
import javax.inject.Singleton;

/** Root Dagger component; external singletons are supplied at creation time. */
@Singleton
@Component(modules = {ManagerModule.class, ManagerBindings.class})
public interface ManagerComponent {

    ConsumerRunner consumerRunner();

    PgHealthProbe pgHealthProbe();

    @Component.Factory
    interface Factory {
        ManagerComponent create(
                @BindsInstance ManagerConfig config,
                @BindsInstance PrometheusMeterRegistry registry,
                @BindsInstance Vertx vertx,
                @BindsInstance @Named("instanceId") String instanceId);
    }
}
