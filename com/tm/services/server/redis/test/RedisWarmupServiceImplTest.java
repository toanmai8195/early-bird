package com.tm.services.server.redis;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tm.common.metric.Metrics;
import com.tm.common.metric.MicrometerMetrics;
import com.tm.services.server.dao.Opportunity;
import com.tm.services.server.dao.ReconciliationDao;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.impl.types.NumberType;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;

public class RedisWarmupServiceImplTest {

    private static final Opportunity OPP =
            new Opportunity("opp-1", "region-1", "zone-1", 1000, 999, 100L, 200L);

    private ReconciliationDao reconciliation;
    private RedisAPI redis;
    private SimpleMeterRegistry registry;
    private Metrics metrics;
    private RedisWarmupServiceImpl service;

    @Before
    public void setUp() {
        reconciliation = mock(ReconciliationDao.class);
        redis = mock(RedisAPI.class);
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerMetrics(registry);
        service = new RedisWarmupServiceImpl(reconciliation, redis, metrics);
    }

    @Test
    public void skipsRebuildWhenHeartbeatPresent() throws Exception {
        when(redis.exists(List.of("warmup:heartbeat"))).thenReturn(Future.succeededFuture(intResponse(1)));

        await(service.reconcile());

        verify(reconciliation, never()).listOpenOpportunities();
        verify(redis, never()).hset(any());
        assertCounter("skipped", 1.0);
    }

    @Test
    public void rebuildsAllOpenOpportunitiesAndSetsHeartbeatWhenMissing() throws Exception {
        when(redis.exists(List.of("warmup:heartbeat"))).thenReturn(Future.succeededFuture(intResponse(0)));
        when(reconciliation.listOpenOpportunities()).thenReturn(Future.succeededFuture(List.of(OPP)));
        when(redis.hset(any())).thenReturn(Future.succeededFuture(mock(Response.class)));
        when(redis.expireat(any())).thenReturn(Future.succeededFuture(mock(Response.class)));
        when(reconciliation.claimedDrivers("opp-1")).thenReturn(Future.succeededFuture(List.of("driver-1", "driver-2")));
        when(redis.sadd(any())).thenReturn(Future.succeededFuture(mock(Response.class)));
        when(redis.set(any())).thenReturn(Future.succeededFuture(mock(Response.class)));

        await(service.reconcile());

        verify(redis).hset(List.of("opp_meta:opp-1", "capacity", "1000", "window_start", "100"));
        verify(redis).expireat(List.of("opp_meta:opp-1", "200"));
        verify(redis).sadd(List.of("claimed_set:opp-1", "driver-1", "driver-2"));
        verify(redis).set(List.of("warmup:heartbeat", "1"));
        assertCounter("restored", 1.0);
    }

    @Test
    public void skipsClaimedSetSaddWhenNoDrivers() throws Exception {
        when(redis.exists(List.of("warmup:heartbeat"))).thenReturn(Future.succeededFuture(intResponse(0)));
        when(reconciliation.listOpenOpportunities()).thenReturn(Future.succeededFuture(List.of(OPP)));
        when(redis.hset(any())).thenReturn(Future.succeededFuture(mock(Response.class)));
        when(redis.expireat(any())).thenReturn(Future.succeededFuture(mock(Response.class)));
        when(reconciliation.claimedDrivers("opp-1")).thenReturn(Future.succeededFuture(List.of()));
        when(redis.set(any())).thenReturn(Future.succeededFuture(mock(Response.class)));

        await(service.reconcile());

        verify(redis, never()).sadd(any());
        verify(redis).set(List.of("warmup:heartbeat", "1"));
        assertCounter("restored", 1.0);
    }

    private static Response intResponse(int value) {
        return NumberType.create(value);
    }

    private static <T> T await(Future<T> future) throws Exception {
        CompletableFuture<T> cf = future.toCompletionStage().toCompletableFuture();
        return cf.get();
    }

    private void assertCounter(String result, double expected) {
        double actual = registry.get("booking.redis.warmup").tag("result", result).counter().count();
        org.junit.Assert.assertEquals(expected, actual, 0.0001);
    }
}
