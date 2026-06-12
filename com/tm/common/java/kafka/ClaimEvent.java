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
}
