package com.tm.common.pg;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ClaimStoreTest {

    private Connection conn;
    private PreparedStatement decrement;
    private PreparedStatement insert;

    @Before
    public void setUp() throws Exception {
        conn = Mockito.mock(Connection.class);
        decrement = Mockito.mock(PreparedStatement.class);
        insert = Mockito.mock(PreparedStatement.class);
        when(conn.prepareStatement(Mockito.contains("UPDATE opportunities"))).thenReturn(decrement);
        when(conn.prepareStatement(Mockito.contains("INSERT INTO bookings"))).thenReturn(insert);
    }

    @Test
    public void commitsAndInsertsWhenCapacityAvailable() throws Exception {
        when(decrement.executeUpdate()).thenReturn(1);

        ClaimStore store = new ClaimStore(conn);
        assertTrue(store.commitClaim("opp-1", "d1", "idem-1"));

        verify(insert, times(1)).executeUpdate();
        verify(conn, times(1)).commit();
        verify(conn, never()).rollback();
    }

    @Test
    public void rejectsWhenCapacityExhausted() throws Exception {
        when(decrement.executeUpdate()).thenReturn(0); // remaining = 0, no oversell

        ClaimStore store = new ClaimStore(conn);
        assertFalse(store.commitClaim("opp-1", "d1", "idem-1"));

        verify(insert, never()).executeUpdate();
        verify(conn, times(1)).rollback();
        verify(conn, never()).commit();
    }
}
