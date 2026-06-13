package com.tm.common.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

/**
 * Shared booking metrics. Counters are tagged by outcome; latency timers publish
 * the P99 percentile so SLO/alerting can consume them (see
 * .claude/rules/observability-metrics.md).
 */
public interface Metrics {

    /** Outcome-tagged counter, e.g. counter("booking.dao.commit", "committed"). */
    Counter counter(String name, String result);

    /**
     * Outcome-tagged counter with an additional {@code instance} label, e.g.
     * counter("booking.consumer.handle", "committed", "manager-0").
     * Falls back to {@link #counter(String, String)} when unimplemented.
     */
    default Counter counter(String name, String result, String instanceId) {
        return counter(name, result);
    }

    /** Latency timer publishing P99, e.g. timer("booking.dao.commit.latency"). */
    Timer timer(String name);

    /** Result-tagged latency timer publishing P99, e.g. timer("booking.api.claim.latency", "ok"). */
    default Timer timer(String name, String result) {
        return timer(name);
    }
}
