package com.tm.services.server.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.tm.common.metric.Metrics;
import com.tm.common.metric.MicrometerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class OpportunityDaoTest {

    private static final Opportunity OPP =
            new Opportunity("opp-1", "region-1", "zone-1", 1000, 1000, 100L, 200L);

    private SimpleMeterRegistry registry;
    private Metrics metrics;
    private DataSource dataSource;
    private Connection conn;
    private PreparedStatement ps;
    private ResultSet rs;
    private RedisAPI redis;
    private Vertx vertx;
    private WorkerExecutor workerExecutor;

    @Before
    public void setUp() throws Exception {
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerMetrics(registry);
        dataSource = Mockito.mock(DataSource.class);
        conn = Mockito.mock(Connection.class);
        ps = Mockito.mock(PreparedStatement.class);
        rs = Mockito.mock(ResultSet.class);
        when(dataSource.getConnection()).thenReturn(conn);
        when(conn.prepareStatement(Mockito.anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);

        redis = Mockito.mock(RedisAPI.class);
        when(redis.hset(any())).thenReturn(Future.succeededFuture(Mockito.mock(Response.class)));
        when(redis.expireat(any())).thenReturn(Future.succeededFuture(Mockito.mock(Response.class)));
        when(redis.del(any())).thenReturn(Future.succeededFuture(Mockito.mock(Response.class)));

        vertx = Vertx.vertx();
        workerExecutor = vertx.createSharedWorkerExecutor("test-pg-opportunity", 4);
    }

    @After
    public void tearDown() {
        workerExecutor.close();
        vertx.close();
    }

    private JdbcOpportunityDao dao() {
        return new JdbcOpportunityDao(dataSource, redis, metrics, workerExecutor);
    }

    @Test
    public void createInsertsRowAndSyncsRedisMeta() throws Exception {
        Opportunity created = await(dao().create(OPP));

        assertEquals(OPP, created);
        Mockito.verify(ps).executeUpdate();
        Mockito.verify(redis).hset(any());
        Mockito.verify(redis).expireat(any());
        assertCounter("create_ok", 1.0);
    }

    @Test
    public void getReturnsEmptyWhenNotFound() throws Exception {
        when(rs.next()).thenReturn(false);

        Optional<Opportunity> result = await(dao().get("opp-1"));

        assertTrue(result.isEmpty());
        assertCounter("get_ok", 1.0);
    }

    @Test
    public void getReturnsOpportunityWhenFound() throws Exception {
        when(rs.next()).thenReturn(true);
        when(rs.getString(1)).thenReturn("region-1");
        when(rs.getString(2)).thenReturn("zone-1");
        when(rs.getLong(3)).thenReturn(100L);
        when(rs.getLong(4)).thenReturn(200L);
        when(rs.getInt(5)).thenReturn(1000);
        when(rs.getInt(6)).thenReturn(1000);

        Optional<Opportunity> result = await(dao().get("opp-1"));

        assertEquals(Optional.of(OPP), result);
        assertCounter("get_ok", 1.0);
    }

    @Test
    public void updateReturnsEmptyAndSkipsRedisWhenNotFound() throws Exception {
        when(rs.next()).thenReturn(false);

        Optional<Opportunity> result = await(dao().update("opp-1", OPP));

        assertTrue(result.isEmpty());
        Mockito.verify(redis, Mockito.never()).hset(any());
        assertCounter("update_ok", 1.0);
    }

    @Test
    public void updateSyncsRedisMetaWhenFound() throws Exception {
        when(rs.next()).thenReturn(true);
        when(rs.getString(1)).thenReturn("region-1");
        when(rs.getString(2)).thenReturn("zone-1");
        when(rs.getLong(3)).thenReturn(100L);
        when(rs.getLong(4)).thenReturn(200L);
        when(rs.getInt(5)).thenReturn(1000);
        when(rs.getInt(6)).thenReturn(1000);

        Optional<Opportunity> result = await(dao().update("opp-1", OPP));

        assertEquals(Optional.of(OPP), result);
        Mockito.verify(redis).hset(any());
        Mockito.verify(redis).expireat(any());
        assertCounter("update_ok", 1.0);
    }

    @Test
    public void deleteReturnsFalseAndSkipsRedisWhenNotFound() throws Exception {
        when(ps.executeUpdate()).thenReturn(0);

        boolean deleted = await(dao().delete("opp-1"));

        assertFalse(deleted);
        Mockito.verify(redis, Mockito.never()).del(any());
        assertCounter("delete_ok", 1.0);
    }

    @Test
    public void deleteClearsRedisMetaAndClaimedSetWhenFound() throws Exception {
        when(ps.executeUpdate()).thenReturn(1);

        boolean deleted = await(dao().delete("opp-1"));

        assertTrue(deleted);
        Mockito.verify(redis).del(any());
        assertCounter("delete_ok", 1.0);
    }

    private static <T> T await(Future<T> future) throws Exception {
        CompletableFuture<T> cf = future.toCompletionStage().toCompletableFuture();
        return cf.get();
    }

    private void assertCounter(String result, double expected) {
        double actual = registry.get("booking.dao.opportunity").tag("result", result).counter().count();
        assertEquals(expected, actual, 0.0001);
    }
}
