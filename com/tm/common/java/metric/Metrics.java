package com.tm.common.metric;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

/**
 * Shared booking metrics. Counters are tagged by outcome; latency timers publish
 * the P99 percentile so SLO/alerting can consume them (see
 * .claude/rules/observability-metrics.md).
 *
 * <p>Beyond {@code result}, both server and manager tag every metric with a
 * {@code caller} label (the {@code X-Caller-Id} carried end-to-end, e.g. the
 * load-test scenario) and the manager adds an {@code instance} label. Use the
 * {@link Tags} overloads to attach those; the simple overloads exist for the few
 * metrics that only carry a {@code result}.
 */
public interface Metrics {

    /** Outcome-tagged counter, e.g. counter("booking.dao.commit", "committed"). */
    Counter counter(String name, String result);

    /**
     * Counter with arbitrary tags, e.g.
     * counter("booking.consumer.handle", Tags.of("result", "committed", "instance", id, "caller", c)).
     */
    Counter counter(String name, Tags tags);

    /** Latency timer publishing P99, e.g. timer("booking.dao.commit.latency"). */
    Timer timer(String name);

    /** Latency timer publishing P99, with arbitrary tags (e.g. result + caller). */
    Timer timer(String name, Tags tags);
}
