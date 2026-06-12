package com.tm.common.pg;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tm.common.kafka.ClaimEvent;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ClaimStoreTest {

    private Connection conn;
    private PreparedStatement ps;
    private ResultSet rs;

    @Before
    public void setUp() throws Exception {
        conn = Mockito.mock(Connection.class);
        ps = Mockito.mock(PreparedStatement.class);
        rs = Mockito.mock(ResultSet.class);
        when(conn.prepareStatement(Mockito.anyString())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
    }

    @Test
    public void mapsPerDriverOutcomeToEventsWithOneBulkStatement() throws Exception {
        // one opportunity -> one bulk statement; d1 appears twice (intra-batch dup)
        List<ClaimEvent> events = List.of(
                new ClaimEvent("opp-1", "d1", "i1"),
                new ClaimEvent("opp-1", "d2", "i2"),
                new ClaimEvent("opp-1", "d1", "i1"));
        // SQL returns one row per distinct driver: (driver, outcome)
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getString(1)).thenReturn("d1", "d2");
        when(rs.getString(2)).thenReturn("COMMITTED", "REJECTED");

        List<ClaimStore.Outcome> outcomes = new PgClaimStore().settleOpportunity(conn, "opp-1", events);

        // both d1 events -> COMMITTED, d2 -> REJECTED, in input order
        assertEquals(List.of(
                ClaimStore.Outcome.COMMITTED,
                ClaimStore.Outcome.REJECTED,
                ClaimStore.Outcome.COMMITTED), outcomes);
        verify(ps, times(1)).executeQuery(); // one bulk statement for the opp
    }

    @Test(expected = java.sql.SQLException.class)
    public void propagatesSqlException() throws Exception {
        when(ps.executeQuery()).thenThrow(new java.sql.SQLException("boom"));
        new PgClaimStore().settleOpportunity(conn, "opp-1", List.of(new ClaimEvent("opp-1", "d1", "i1")));
    }
}
