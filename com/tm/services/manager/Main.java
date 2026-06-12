package com.tm.services.manager;

import com.tm.services.manager.config.ManagerConfig;
import com.tm.services.manager.di.DaggerManagerComponent;
import com.tm.services.manager.di.ManagerComponent;

/**
 * Manager entrypoint: parse config, build the Dagger object graph, run the poll
 * loop. All construction lives in {@link com.tm.services.manager.di.ManagerModule}.
 */
public final class Main {

    public static void main(String[] args) {
        ManagerConfig config = ManagerConfig.load(args);
        ManagerComponent component = DaggerManagerComponent.factory().create(config);
        component.consumerRunner().run();
    }
}
