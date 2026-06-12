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

    /** Latency timer publishing P99, e.g. timer("booking.dao.commit.latency"). */
    Timer timer(String name);
}
