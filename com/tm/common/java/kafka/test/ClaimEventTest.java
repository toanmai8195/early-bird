package com.tm.common.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ClaimEventTest {

    @Test
    public void toJsonContainsAllFields() {
        ClaimEvent e = new ClaimEvent("opp-1", "driver-9", "idem-abc", 0L);
        String json = e.toJson();
        assertTrue(json.contains("\"opportunity_id\":\"opp-1\""));
        assertTrue(json.contains("\"driver_id\":\"driver-9\""));
        assertTrue(json.contains("\"idempotency_key\":\"idem-abc\""));
    }

    @Test
    public void accessorsExposeComponents() {
        ClaimEvent e = new ClaimEvent("opp-1", "driver-9", "idem-abc", 0L);
        assertEquals("opp-1", e.opportunityId());
        assertEquals("driver-9", e.driverId());
        assertEquals("idem-abc", e.idempotencyKey());
    }

    @Test
    public void fromJsonRoundTrips() {
        ClaimEvent original = new ClaimEvent("opp-1", "driver-9", "idem-abc", 0L);
        ClaimEvent parsed = ClaimEvent.fromJson(original.toJson());
        assertEquals(original, parsed);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fromJsonRejectsMissingField() {
        ClaimEvent.fromJson("{\"opportunity_id\":\"opp-1\"}");
    }
}
