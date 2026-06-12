package com.tm.common.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/** Shared booking metrics: claim outcomes, gate latency, pending depth. */
public final class Metrics {

    private final MeterRegistry registry;
    private final Timer gateLatency;

    public Metrics(MeterRegistry registry) {
        this.registry = registry;
        this.gateLatency = Timer.builder("booking.gate.latency").register(registry);
    }

    /** result = ok | full | dup | error */
    public void claimOutcome(String result) {
        Counter.builder("booking.claim.outcome").tag("result", result).register(registry).increment();
    }

    public Timer gateLatency() {
        return gateLatency;
    }
}
