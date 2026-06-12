package com.tm.services.manager.di;

import com.tm.services.manager.ConsumerRunner;
import com.tm.services.manager.config.ManagerConfig;
import dagger.BindsInstance;
import dagger.Component;

import javax.inject.Singleton;

/** Root Dagger component; {@link ManagerConfig} is supplied at creation time. */
@Singleton
@Component(modules = {ManagerModule.class, ManagerBindings.class})
public interface ManagerComponent {

    ConsumerRunner consumerRunner();

    @Component.Factory
    interface Factory {
        ManagerComponent create(@BindsInstance ManagerConfig config);
    }
}
