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
        when(redis.eval(any())).thenReturn(Future.succeededFuture(resp));
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
    public void evalUsesPerOpportunityClaimedSetAndMetaKeys() {
        RedisAPI redis = redisReturning("OK");
        new VertxClaimGate(redis).claim("opp-42", "driver-7").result();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> args = ArgumentCaptor.forClass(List.class);
        Mockito.verify(redis).eval(args.capture());

        List<String> a = args.getValue();
        // [script, numkeys, claimed_set key, opp_meta key, driver_id]
        assertEquals("2", a.get(1));
        assertEquals("claimed_set:opp-42", a.get(2));
        assertEquals("opp_meta:opp-42", a.get(3));
        assertEquals("driver-7", a.get(4));
    }
}
