package com.tm.common.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
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
        return counter(name, Tags.of("result", result));
    }

    @Override
    public Counter counter(String name, Tags tags) {
        return Counter.builder(name).tags(tags).register(registry);
    }

    @Override
    public Timer timer(String name) {
        return timer(name, Tags.empty());
    }

    @Override
    public Timer timer(String name, Tags tags) {
        return Timer.builder(name).tags(tags).publishPercentiles(0.99).register(registry);
    }
}
