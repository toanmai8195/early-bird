package com.tm.common.metric;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

public class MetricsTest {

    @Test
    public void counterIncrementsTaggedByResult() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics metrics = new MicrometerMetrics(registry);

        metrics.counter("booking.dao.commit", "committed").increment();
        metrics.counter("booking.dao.commit", "committed").increment();
        metrics.counter("booking.dao.commit", "rejected").increment();

        assertEquals(2.0,
                registry.get("booking.dao.commit").tag("result", "committed").counter().count(), 0.0001);
        assertEquals(1.0,
                registry.get("booking.dao.commit").tag("result", "rejected").counter().count(), 0.0001);
    }

    @Test
    public void counterWithInstanceIdAddsInstanceTag() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics metrics = new MicrometerMetrics(registry);

        metrics.counter("booking.consumer.handle", "committed", "manager-0").increment();
        metrics.counter("booking.consumer.handle", "committed", "manager-1").increment();
        metrics.counter("booking.consumer.handle", "committed", "manager-1").increment();

        assertEquals(1.0,
                registry.get("booking.consumer.handle")
                        .tag("result", "committed").tag("instance", "manager-0").counter().count(), 0.0001);
        assertEquals(2.0,
                registry.get("booking.consumer.handle")
                        .tag("result", "committed").tag("instance", "manager-1").counter().count(), 0.0001);
    }

    @Test
    public void timerRecordsAndPublishesP99() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Metrics metrics = new MicrometerMetrics(registry);

        metrics.timer("booking.dao.commit.latency").record(java.time.Duration.ofMillis(5));
        var timer = registry.get("booking.dao.commit.latency").timer();

        assertEquals(1, timer.count());
        boolean hasP99 = false;
        for (var p : timer.takeSnapshot().percentileValues()) {
            if (Math.abs(p.percentile() - 0.99) < 1e-9) {
                hasP99 = true;
            }
        }
        assertTrue("timer must publish P99 percentile", hasP99);
    }
}
