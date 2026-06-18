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
 *
 * <p>{@code callerId} identifies who issued the claim (e.g. the load-test scenario:
 * {@code contended} / {@code diverse} / {@code realistic}). It rides the whole flow
 * — set from the {@code X-Caller-Id} header on the server, carried through Kafka, and
 * used by the manager to tag every metric — so API latency (load-test side) and PG
 * settle latency (manager side) can be compared per caller. Defaults to
 * {@link #UNKNOWN_CALLER} for messages/tests that don't set it.
 */
public record ClaimEvent(String opportunityId, String driverId, String idempotencyKey,
                         long serverReceivedAt, String callerId) {

    /** Caller used when none is supplied (old messages, internal callers, tests). */
    public static final String UNKNOWN_CALLER = "unknown";

    /** Backward-compatible constructor without a caller: defaults to {@link #UNKNOWN_CALLER}. */
    public ClaimEvent(String opportunityId, String driverId, String idempotencyKey, long serverReceivedAt) {
        this(opportunityId, driverId, idempotencyKey, serverReceivedAt, UNKNOWN_CALLER);
    }

    /** Creates an event timestamped at the current wall-clock time, with a caller. */
    public static ClaimEvent now(String opportunityId, String driverId, String idempotencyKey, String callerId) {
        return new ClaimEvent(opportunityId, driverId, idempotencyKey, System.currentTimeMillis(),
                callerId == null || callerId.isBlank() ? UNKNOWN_CALLER : callerId);
    }

    public String toJson() {
        return "{\"opportunity_id\":\"" + opportunityId +
               "\",\"driver_id\":\"" + driverId +
               "\",\"idempotency_key\":\"" + idempotencyKey +
               "\",\"caller_id\":\"" + callerId +
               "\",\"server_received_at\":" + serverReceivedAt + "}";
    }

    /**
     * Parses the flat JSON produced by {@link #toJson()}. Kept dependency-free
     * because the schema is small and fully controlled by this codebase.
     * {@code server_received_at} defaults to 0 and {@code caller_id} to
     * {@link #UNKNOWN_CALLER} when absent (old messages / tests).
     */
    public static ClaimEvent fromJson(String json) {
        long ts = 0;
        String tsMarker = "\"server_received_at\":";
        int tsIdx = json.indexOf(tsMarker);
        if (tsIdx >= 0) {
            // Scan digits directly — avoids compiling a regex per Kafka record on the
            // manager's hot poll path (this runs once per consumed message).
            int i = tsIdx + tsMarker.length();
            long acc = 0;
            boolean seen = false;
            while (i < json.length()) {
                char c = json.charAt(i++);
                if (c >= '0' && c <= '9') {
                    acc = acc * 10 + (c - '0');
                    seen = true;
                } else if (seen) {
                    break;
                }
            }
            if (seen) ts = acc;
        }
        return new ClaimEvent(
                field(json, "opportunity_id"),
                field(json, "driver_id"),
                field(json, "idempotency_key"),
                ts,
                optionalField(json, "caller_id", UNKNOWN_CALLER));
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

    /** Like {@link #field} but returns {@code fallback} instead of throwing when absent. */
    private static String optionalField(String json, String key, String fallback) {
        String marker = "\"" + key + "\":\"";
        int start = json.indexOf(marker);
        if (start < 0) {
            return fallback;
        }
        start += marker.length();
        int end = json.indexOf('"', start);
        return end < 0 ? fallback : json.substring(start, end);
    }
}
