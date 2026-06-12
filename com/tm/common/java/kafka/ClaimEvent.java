package com.tm.common.kafka;

/**
 * Message handed off from server to manager. Capacity is already decided at the
 * Redis gate, so events need no ordering; the producer partitions round-robin
 * to avoid hot partitions on a single hot opportunity.
 */
public record ClaimEvent(String opportunityId, String driverId, String idempotencyKey) {

    public String toJson() {
        return "{\"opportunity_id\":\"" + opportunityId +
               "\",\"driver_id\":\"" + driverId +
               "\",\"idempotency_key\":\"" + idempotencyKey + "\"}";
    }

    /**
     * Parses the flat JSON produced by {@link #toJson()}. Kept dependency-free
     * because the schema is small and fully controlled by this codebase.
     */
    public static ClaimEvent fromJson(String json) {
        return new ClaimEvent(
                field(json, "opportunity_id"),
                field(json, "driver_id"),
                field(json, "idempotency_key"));
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
