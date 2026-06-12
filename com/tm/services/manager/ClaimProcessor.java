package com.tm.services.manager;

import com.tm.common.kafka.ClaimEvent;
import com.tm.common.pg.ClaimStore;

import java.sql.SQLException;

/**
 * Commits one claim event to Postgres (source of truth + backstop). On success
 * it removes the entry from the Redis pending set and notifies the driver app
 * (MQTT/WebSocket if online, push if offline). Idempotent under Kafka
 * at-least-once delivery via the UNIQUE(opportunity_id, driver_id) constraint.
 */
public final class ClaimProcessor {

    private final ClaimStore store;

    public ClaimProcessor(ClaimStore store) {
        this.store = store;
    }

    public void handle(ClaimEvent event) throws SQLException {
        boolean committed = store.commitClaim(
                event.opportunityId(), event.driverId(), event.idempotencyKey());
        if (committed) {
            // TODO: remove from Redis pending set + notify driver app.
        }
    }
}
