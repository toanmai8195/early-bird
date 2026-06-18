package com.tm.common.redis;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import java.util.List;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ClaimGateTest {

    private RedisAPI redisReturning(String scriptResult) {
        RedisAPI redis = Mockito.mock(RedisAPI.class);
        Response resp = Mockito.mock(Response.class);
        when(resp.toString()).thenReturn(scriptResult);
        when(redis.evalsha(any())).thenReturn(Future.succeededFuture(resp));
        return redis;
    }

    @Test
    public void mapsScriptReturnToResult() {
        assertEquals(ClaimGate.Result.OK,
                new VertxClaimGate(redisReturning("OK")).claim("opp-1", "d1").result());
        assertEquals(ClaimGate.Result.FULL,
                new VertxClaimGate(redisReturning("FULL")).claim("opp-1", "d2").result());
        assertEquals(ClaimGate.Result.DUP,
                new VertxClaimGate(redisReturning("DUP")).claim("opp-1", "d1").result());
        assertEquals(ClaimGate.Result.CLOSED,
                new VertxClaimGate(redisReturning("CLOSED")).claim("opp-1", "d1").result());
        assertEquals(ClaimGate.Result.DOWN,
                new VertxClaimGate(redisReturning("DOWN")).claim("opp-1", "d1").result());
    }

    @Test
    public void releaseRemovesDriverFromClaimedSet() {
        RedisAPI redis = Mockito.mock(RedisAPI.class);
        when(redis.srem(any())).thenReturn(Future.succeededFuture(Mockito.mock(Response.class)));

        new VertxClaimGate(redis).release("opp-42", "driver-7").result();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> args = ArgumentCaptor.forClass(List.class);
        Mockito.verify(redis).srem(args.capture());

        List<String> a = args.getValue();
        assertEquals("claimed_set:opp-42", a.get(0));
        assertEquals("driver-7", a.get(1));
    }

    @Test
    public void releaseAllRemovesAllDriversInOneSrem() {
        RedisAPI redis = Mockito.mock(RedisAPI.class);
        when(redis.srem(any())).thenReturn(Future.succeededFuture(Mockito.mock(Response.class)));

        new VertxClaimGate(redis).releaseAll("opp-42", List.of("d1", "d2", "d3")).result();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> args = ArgumentCaptor.forClass(List.class);
        Mockito.verify(redis, Mockito.times(1)).srem(args.capture());
        assertEquals(List.of("claimed_set:opp-42", "d1", "d2", "d3"), args.getValue());
    }

    @Test
    public void releaseAllEmptyIsNoOp() {
        RedisAPI redis = Mockito.mock(RedisAPI.class);
        new VertxClaimGate(redis).releaseAll("opp-42", List.of()).result();
        Mockito.verify(redis, Mockito.never()).srem(any());
    }

    @Test
    public void rejectAllRemovesDriversAndDecrementsCapacityByCount() {
        RedisAPI redis = Mockito.mock(RedisAPI.class);
        when(redis.srem(any())).thenReturn(Future.succeededFuture(Mockito.mock(Response.class)));
        when(redis.hincrby(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(Future.succeededFuture(Mockito.mock(Response.class)));

        new VertxClaimGate(redis).rejectAll("opp-42", List.of("d1", "d2", "d3")).result();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> sremArgs = ArgumentCaptor.forClass(List.class);
        Mockito.verify(redis, Mockito.times(1)).srem(sremArgs.capture());
        assertEquals(List.of("claimed_set:opp-42", "d1", "d2", "d3"), sremArgs.getValue());
        // Capacity decremented once, by the number of rejected drivers.
        Mockito.verify(redis).hincrby("opp_meta:opp-42", "capacity", "-3");
    }

    @Test
    public void claimUsesEvalshaWithPerOpportunityKeys() {
        RedisAPI redis = redisReturning("OK");
        new VertxClaimGate(redis).claim("opp-42", "driver-7").result();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> args = ArgumentCaptor.forClass(List.class);
        Mockito.verify(redis).evalsha(args.capture());

        List<String> a = args.getValue();
        // [sha, numkeys, claimed_set key, opp_meta key, pg_health key, driver_id]
        assertEquals(40, a.get(0).length()); // SHA1 hex
        assertEquals("3", a.get(1));
        assertEquals("claimed_set:opp-42", a.get(2));
        assertEquals("opp_meta:opp-42", a.get(3));
        assertEquals(PgHealth.KEY, a.get(4));
        assertEquals("driver-7", a.get(5));
    }

    @Test
    public void claimFallsBackToEvalOnNoScript() {
        RedisAPI redis = Mockito.mock(RedisAPI.class);
        when(redis.evalsha(any()))
                .thenReturn(Future.failedFuture(new RuntimeException("NOSCRIPT No matching script")));
        Response resp = Mockito.mock(Response.class);
        when(resp.toString()).thenReturn("OK");
        when(redis.eval(any())).thenReturn(Future.succeededFuture(resp));

        assertEquals(ClaimGate.Result.OK,
                new VertxClaimGate(redis).claim("opp-42", "driver-7").result());

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> args = ArgumentCaptor.forClass(List.class);
        Mockito.verify(redis).eval(args.capture());
        // EVAL is sent the full script body (not the SHA) so Redis re-caches it.
        org.junit.Assert.assertTrue(args.getValue().get(0).contains("redis.call"));
    }
}
