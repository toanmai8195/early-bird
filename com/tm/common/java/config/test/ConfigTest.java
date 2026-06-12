package com.tm.common.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class ConfigTest {

    @Test
    public void cliFlagOverridesDefault() {
        Config cfg = Config.from(new String[]{"--poll-timeout-ms=1000"})
                .with("poll-timeout-ms", "POLL_TIMEOUT_MS", "500")
                .build();
        assertEquals(1000L, cfg.getLong("poll-timeout-ms"));
    }

    @Test
    public void fallsBackToDefaultWhenNoCliOrEnv() {
        // ENV var name unlikely to be set in the test environment.
        Config cfg = Config.from(new String[]{})
                .with("max-poll-records", "EARLYBIRD_UNSET_ENV_XYZ", "500")
                .build();
        assertEquals(500, cfg.getInt("max-poll-records"));
    }

    @Test
    public void getReturnsRawString() {
        Config cfg = Config.from(new String[]{"--topic=claim-events"})
                .with("topic", "TOPIC", "default-topic")
                .build();
        assertEquals("claim-events", cfg.get("topic"));
    }

    @Test
    public void ignoresMalformedArgs() {
        Config cfg = Config.from(new String[]{"--no-equals", "positional", "--k=v"})
                .with("k", "K_ENV", "def")
                .build();
        assertEquals("v", cfg.get("k"));
    }

    @Test
    public void unknownKeyThrows() {
        Config cfg = Config.from(new String[]{}).build();
        assertThrows(IllegalArgumentException.class, () -> cfg.get("missing"));
    }
}
