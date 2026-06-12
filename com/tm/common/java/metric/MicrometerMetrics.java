package com.tm.common.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import javax.inject.Inject;
import javax.inject.Singleton;

/** Micrometer-backed {@link Metrics}. */
@Singleton
public final class MicrometerMetrics implements Metrics {

    private final MeterRegistry registry;

    @Inject
    public MicrometerMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Counter counter(String name, String result) {
        return Counter.builder(name).tag("result", result).register(registry);
    }

    @Override
    public Timer timer(String name) {
        return Timer.builder(name).publishPercentiles(0.99).register(registry);
    }
}
