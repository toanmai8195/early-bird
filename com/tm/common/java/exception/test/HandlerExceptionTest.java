package com.tm.common.exception;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.junit.Test;

public class HandlerExceptionTest {

    @Test
    public void carriesMessageAndCause() {
        Throwable cause = new IllegalStateException("boom");
        HandlerException e = new HandlerException("failed", cause);
        assertEquals("failed", e.getMessage());
        assertSame(cause, e.getCause());
    }
}
