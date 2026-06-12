package com.tm.services.server.dao;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.tm.common.metric.Metrics;
import com.tm.common.metric.MicrometerMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class JdbcReconciliationDaoTest {

    private SimpleMeterRegistry registry;
    private Metrics metrics;
    private DataSource dataSource;
    private Connection conn;
    private PreparedStatement ps;
    private ResultSet rs;
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

        vertx = Vertx.vertx();
        workerExecutor = vertx.createSharedWorkerExecutor("test-pg-reconciliation", 4);
    }

    @After
    public void tearDown() {
        workerExecutor.close();
        vertx.close();
    }

    private JdbcReconciliationDao dao() {
        return new JdbcReconciliationDao(dataSource, metrics, workerExecutor);
    }

    @Test
    public void listOpenOpportunitiesReturnsCurrentlyOpenRows() throws Exception {
        when(rs.next()).thenReturn(true, false);
        when(rs.getString(1)).thenReturn("opp-1");
        when(rs.getString(2)).thenReturn("region-1");
        when(rs.getString(3)).thenReturn("zone-1");
        when(rs.getLong(4)).thenReturn(100L);
        when(rs.getLong(5)).thenReturn(200L);
        when(rs.getInt(6)).thenReturn(1000);
        when(rs.getInt(7)).thenReturn(999);

        List<Opportunity> result = await(dao().listOpenOpportunities());

        assertEquals(List.of(new Opportunity("opp-1", "region-1", "zone-1", 1000, 999, 100L, 200L)), result);
        assertCounter("list_open_ok", 1.0);
    }

    @Test
    public void claimedDriversReturnsDriverIds() throws Exception {
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString(1)).thenReturn("driver-1", "driver-2");

        List<String> result = await(dao().claimedDrivers("opp-1"));

        assertEquals(List.of("driver-1", "driver-2"), result);
        assertCounter("claimed_drivers_ok", 1.0);
    }

    private static <T> T await(Future<T> future) throws Exception {
        CompletableFuture<T> cf = future.toCompletionStage().toCompletableFuture();
        return cf.get();
    }

    private void assertCounter(String result, double expected) {
        double actual = registry.get("booking.dao.reconciliation").tag("result", result).counter().count();
        assertEquals(expected, actual, 0.0001);
    }
}
