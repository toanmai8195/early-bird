package com.tm.common.kafka;

/**
 * Message handed off from server to manager. Capacity is already decided at the
 * Redis gate, so events need no ordering; the producer keys by {@code opportunity_id}
 * so every claim of one opportunity lands on the same partition/consumer, letting the
 * manager group + bulk-settle one statement per opportunity per poll (trading off a hot
 * partition for a hot opportunity).
 *
 * <p>{@code serverReceivedAt} is the epoch-millisecond timestamp when the server
 * received the HTTP request. The manager reads it to compute end-to-end latency
 * (request receipt → driver notified). Use {@link #now} in production; pass 0
 * in tests that don't care about e2e latency.
 */
public record ClaimEvent(String opportunityId, String driverId, String idempotencyKey, long serverReceivedAt) {

    /** Creates an event timestamped at the current wall-clock time. */
    public static ClaimEvent now(String opportunityId, String driverId, String idempotencyKey) {
        return new ClaimEvent(opportunityId, driverId, idempotencyKey, System.currentTimeMillis());
    }

    public String toJson() {
        return "{\"opportunity_id\":\"" + opportunityId +
               "\",\"driver_id\":\"" + driverId +
               "\",\"idempotency_key\":\"" + idempotencyKey +
               "\",\"server_received_at\":" + serverReceivedAt + "}";
    }

    /**
     * Parses the flat JSON produced by {@link #toJson()}. Kept dependency-free
     * because the schema is small and fully controlled by this codebase.
     * {@code server_received_at} defaults to 0 when absent (old messages / tests).
     */
    public static ClaimEvent fromJson(String json) {
        long ts = 0;
        String tsMarker = "\"server_received_at\":";
        int tsIdx = json.indexOf(tsMarker);
        if (tsIdx >= 0) {
            int start = tsIdx + tsMarker.length();
            int end = json.indexOf('}', start);
            if (end < 0) end = json.length();
            String raw = json.substring(start, end).trim().replaceAll("[^0-9]", "");
            if (!raw.isEmpty()) ts = Long.parseLong(raw);
        }
        return new ClaimEvent(
                field(json, "opportunity_id"),
                field(json, "driver_id"),
                field(json, "idempotency_key"),
                ts);
    }

    private static String field(String json, String key) {
        String marker = "\"" + key + "\":\"";
        int start = json.indexOf(marker);
        if (start < 0) {
            throw new IllegalArgumentException("missing field '" + key + "' in: " + json);
        }
        start += marker.length();
        int end = json.indexOf('"', start);
        if (end < 0) {
            throw new IllegalArgumentException("unterminated field '" + key + "' in: " + json);
        }
        return json.substring(start, end);
    }
}
