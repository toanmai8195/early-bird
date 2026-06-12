package com.tm.services.server.dao;

import io.vertx.core.json.JsonObject;

/**
 * Delivery-opportunity DTO. {@code bookingWindowStart}/{@code bookingWindowEnd}
 * are epoch seconds, matching {@code opp_meta:{opp}} in Redis (see
 * {@link com.tm.common.redis.VertxClaimGate}) and {@code opportunities} in PG
 * (see com/tm/infra/migrations/0001_init.sql).
 */
public record Opportunity(
        String opportunityId,
        String regionId,
        String zoneId,
        int capacity,
        int remaining,
        long bookingWindowStart,
        long bookingWindowEnd) {

    public JsonObject toJson() {
        return new JsonObject()
                .put("opportunity_id", opportunityId)
                .put("region_id", regionId)
                .put("zone_id", zoneId)
                .put("capacity", capacity)
                .put("remaining", remaining)
                .put("booking_window_start", bookingWindowStart)
                .put("booking_window_end", bookingWindowEnd);
    }

    /**
     * Parses and validates a create/update request body; {@code opportunityId}
     * comes from the path, not the body. {@code remaining} defaults to
     * {@code capacity} (only meaningful for update, where the caller may want
     * to set it explicitly).
     *
     * @throws IllegalArgumentException if a required field is missing or
     *         out of range; callers should turn this into a 400 response.
     */
    public static Opportunity fromRequest(String opportunityId, JsonObject body) {
        if (opportunityId == null || opportunityId.isBlank()) {
            throw new IllegalArgumentException("opportunity_id is required");
        }
        if (body == null) {
            throw new IllegalArgumentException("request body is required");
        }

        String regionId = body.getString("region_id");
        String zoneId = body.getString("zone_id");
        Integer capacity = body.getInteger("capacity");
        Long windowStart = body.getLong("booking_window_start");
        Long windowEnd = body.getLong("booking_window_end");

        if (regionId == null || regionId.isBlank()) {
            throw new IllegalArgumentException("region_id is required");
        }
        if (zoneId == null || zoneId.isBlank()) {
            throw new IllegalArgumentException("zone_id is required");
        }
        if (capacity == null || capacity <= 0) {
            throw new IllegalArgumentException("capacity must be a positive integer");
        }
        if (windowStart == null || windowEnd == null) {
            throw new IllegalArgumentException("booking_window_start and booking_window_end are required");
        }
        if (windowEnd <= windowStart) {
            throw new IllegalArgumentException("booking_window_end must be after booking_window_start");
        }

        int remaining = body.getInteger("remaining", capacity);
        if (remaining < 0 || remaining > capacity) {
            throw new IllegalArgumentException("remaining must be between 0 and capacity");
        }

        return new Opportunity(opportunityId, regionId, zoneId, capacity, remaining, windowStart, windowEnd);
    }
}
