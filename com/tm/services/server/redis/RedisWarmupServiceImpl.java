package com.tm.services.server.redis;

import com.tm.common.metric.Metrics;
import com.tm.services.server.dao.Opportunity;
import com.tm.services.server.dao.ReconciliationDao;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;

import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * See .claude/rules/observability-metrics.md — records a counter (tagged by
 * outcome: skipped/restored/error) and a P99 latency timer for each
 * {@link #reconcile()} run.
 */
@Singleton
public final class RedisWarmupServiceImpl implements RedisWarmupService {

    private static final String METRIC = "booking.redis.warmup";
    private static final String LATENCY = "booking.redis.warmup.latency";

    /**
     * Sentinel key with no TTL: present iff Redis still has the dataset it had
     * when {@link #restoreAll()} last ran. A full Redis restart/flush wipes this
     * key along with everything else, so a single {@code EXISTS} check is enough
     * to detect data loss without scanning every open opportunity each tick.
     */
    private static final String HEARTBEAT_KEY = "warmup:heartbeat";

    private final ReconciliationDao reconciliation;
    private final RedisAPI redis;
    private final Metrics metrics;

    @Inject
    public RedisWarmupServiceImpl(ReconciliationDao reconciliation, RedisAPI redis, Metrics metrics) {
        this.reconciliation = reconciliation;
        this.redis = redis;
        this.metrics = metrics;
    }

    @Override
    public Future<Void> reconcile() {
        Timer.Sample sample = Timer.start();
        return redis.exists(List.of(HEARTBEAT_KEY))
                .compose(reply -> reply.toInteger() > 0
                        ? Future.succeededFuture("skipped")
                        : restoreAll().map(v -> "restored"))
                .andThen(ar -> {
                    sample.stop(metrics.timer(LATENCY));
                    metrics.counter(METRIC, ar.succeeded() ? ar.result() : "error").increment();
                })
                .mapEmpty();
    }

    /** Rebuilds opp_meta/claimed_set for every currently-open opportunity, then sets the heartbeat. */
    private Future<Void> restoreAll() {
        return reconciliation.listOpenOpportunities()
                .compose(opportunities -> {
                    List<Future<Void>> restores = new ArrayList<>(opportunities.size());
                    for (Opportunity o : opportunities) {
                        restores.add(restore(o));
                    }
                    return Future.join(restores).<Void>mapEmpty();
                })
                .compose(v -> redis.set(List.of(HEARTBEAT_KEY, "1")).mapEmpty());
    }

    private Future<Void> restore(Opportunity o) {
        String metaKey = "opp_meta:" + o.opportunityId();
        String claimedKey = "claimed_set:" + o.opportunityId();
        return redis.hset(List.of(metaKey,
                        "capacity", Integer.toString(o.capacity()),
                        "window_start", Long.toString(o.bookingWindowStart())))
                .compose(v -> redis.expireat(List.of(metaKey, Long.toString(o.bookingWindowEnd()))))
                .compose(v -> reconciliation.claimedDrivers(o.opportunityId()))
                .compose(drivers -> drivers.isEmpty()
                        ? Future.succeededFuture()
                        : redis.sadd(withKey(claimedKey, drivers)).mapEmpty())
                .mapEmpty();
    }

    private static List<String> withKey(String key, List<String> values) {
        List<String> args = new ArrayList<>(values.size() + 1);
        args.add(key);
        args.addAll(values);
        return args;
    }
}
