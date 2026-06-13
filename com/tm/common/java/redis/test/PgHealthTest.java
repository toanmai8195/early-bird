package com.tm.common.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import java.util.List;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class PgHealthTest {

    @Test
    public void markDownSetsFlagWithTtl() {
        RedisAPI redis = Mockito.mock(RedisAPI.class);
        when(redis.set(any())).thenReturn(Future.succeededFuture(Mockito.mock(Response.class)));

        new VertxPgHealth(redis).markDown().result();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> args = ArgumentCaptor.forClass(List.class);
        Mockito.verify(redis).set(args.capture());
        List<String> a = args.getValue();
        assertEquals(PgHealth.KEY, a.get(0));
        assertEquals(PgHealth.DOWN, a.get(1));
        assertEquals("EX", a.get(2));
        assertEquals(Long.toString(PgHealth.DOWN_TTL_SECONDS), a.get(3));
    }

    @Test
    public void markUpDeletesFlag() {
        RedisAPI redis = Mockito.mock(RedisAPI.class);
        when(redis.del(any())).thenReturn(Future.succeededFuture(Mockito.mock(Response.class)));

        new VertxPgHealth(redis).markUp().result();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> args = ArgumentCaptor.forClass(List.class);
        Mockito.verify(redis).del(args.capture());
        assertEquals(PgHealth.KEY, args.getValue().get(0));
    }

    @Test
    public void isHealthyTrueWhenFlagAbsent() {
        assertTrue(healthWithExists(0).isHealthy().result());
    }

    @Test
    public void isHealthyFalseWhenFlagPresent() {
        assertFalse(healthWithExists(1).isHealthy().result());
    }

    private VertxPgHealth healthWithExists(int existsReply) {
        RedisAPI redis = Mockito.mock(RedisAPI.class);
        Response resp = Mockito.mock(Response.class);
        when(resp.toInteger()).thenReturn(existsReply);
        when(redis.exists(any())).thenReturn(Future.succeededFuture(resp));
        return new VertxPgHealth(redis);
    }
}
