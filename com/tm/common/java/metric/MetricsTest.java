package com.tm.common.metric;

import static org.junit.Assert.assertEquals;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

public class MetricsTest {

    @Test
    public void claimOutcomeIncrementsTaggedCounter() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics metrics = new Metrics(registry);

        metrics.claimOutcome("ok");
        metrics.claimOutcome("ok");
        metrics.claimOutcome("full");

        assertEquals(2.0,
                registry.get("booking.claim.outcome").tag("result", "ok").counter().count(), 0.0001);
        assertEquals(1.0,
                registry.get("booking.claim.outcome").tag("result", "full").counter().count(), 0.0001);
    }

    @Test
    public void gateLatencyTimerIsRegistered() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics metrics = new Metrics(registry);
        metrics.gateLatency().record(java.time.Duration.ofMillis(5));
        assertEquals(1, metrics.gateLatency().count());
    }
}
