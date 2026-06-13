package com.tm.services.manager;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tm.services.manager.dao.BookingDao;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.vertx.core.Future;
import org.junit.Test;
import org.mockito.Mockito;

public class PgHealthProbeTest {

    /** One half-open trial so a single probe outcome decides CLOSED vs OPEN. */
    private CircuitBreaker breaker() {
        return CircuitBreaker.of("test", CircuitBreakerConfig.custom()
                .permittedNumberOfCallsInHalfOpenState(1)
                .build());
    }

    @Test
    public void closedBreakerDoesNotProbe() {
        BookingDao dao = Mockito.mock(BookingDao.class);
        new PgHealthProbe(breaker(), dao).probe().result();
        verify(dao, never()).ping();
    }

    @Test
    public void disabledBreakerDoesNotProbe() {
        CircuitBreaker cb = breaker();
        cb.transitionToDisabledState();
        BookingDao dao = Mockito.mock(BookingDao.class);
        new PgHealthProbe(cb, dao).probe().result();
        verify(dao, never()).ping();
    }

    @Test
    public void openBreakerBeforeWaitElapsesDoesNotProbe() {
        CircuitBreaker cb = breaker();
        cb.transitionToOpenState(); // default open-state wait not elapsed
        BookingDao dao = Mockito.mock(BookingDao.class);
        new PgHealthProbe(cb, dao).probe().result();
        verify(dao, never()).ping();
    }

    @Test
    public void halfOpenProbeSuccessClosesBreaker() {
        CircuitBreaker cb = breaker();
        cb.transitionToOpenState();
        cb.transitionToHalfOpenState();
        BookingDao dao = Mockito.mock(BookingDao.class);
        when(dao.ping()).thenReturn(Future.succeededFuture());

        new PgHealthProbe(cb, dao).probe().result();

        verify(dao, times(1)).ping();
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
    }

    @Test
    public void halfOpenProbeFailureReopensBreaker() {
        CircuitBreaker cb = breaker();
        cb.transitionToOpenState();
        cb.transitionToHalfOpenState();
        BookingDao dao = Mockito.mock(BookingDao.class);
        when(dao.ping()).thenReturn(Future.failedFuture(new java.sql.SQLException("still down")));

        new PgHealthProbe(cb, dao).probe().result();

        verify(dao, times(1)).ping();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
    }
}
