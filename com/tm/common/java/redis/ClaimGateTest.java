package com.tm.common.redis;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ClaimGateTest {

    @SuppressWarnings("unchecked")
    private RedisCommands<String, String> redis() {
        return (RedisCommands<String, String>) Mockito.mock(RedisCommands.class);
    }

    @Test
    public void mapsScriptReturnToResult() {
        RedisCommands<String, String> redis = redis();
        when(redis.<String>eval(any(String.class), eq(ScriptOutputType.VALUE),
                any(String[].class), any(String.class), any(String.class)))
                .thenReturn("OK", "FULL", "DUP");

        ClaimGate gate = new ClaimGate(redis);
        assertEquals(ClaimGate.Result.OK, gate.claim("opp-1", "d1", 1000));
        assertEquals(ClaimGate.Result.FULL, gate.claim("opp-1", "d2", 1000));
        assertEquals(ClaimGate.Result.DUP, gate.claim("opp-1", "d1", 1000));
    }

    @Test
    public void usesPerOpportunityKeyAndCapacityArg() {
        RedisCommands<String, String> redis = redis();
        when(redis.<String>eval(any(String.class), any(ScriptOutputType.class),
                any(String[].class), any(String.class), any(String.class)))
                .thenReturn("OK");

        ClaimGate gate = new ClaimGate(redis);
        gate.claim("opp-42", "driver-7", 500);

        ArgumentCaptor<String[]> keys = ArgumentCaptor.forClass(String[].class);
        ArgumentCaptor<String> args = ArgumentCaptor.forClass(String.class);
        Mockito.verify(redis).eval(any(String.class), eq(ScriptOutputType.VALUE),
                keys.capture(), args.capture(), args.capture());

        assertEquals("claimed_set:opp-42", keys.getValue()[0]);
        assertEquals("driver-7", args.getAllValues().get(0));
        assertEquals("500", args.getAllValues().get(1));
    }
}
