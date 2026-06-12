package com.tm.common.pg;

import com.tm.common.kafka.ClaimEvent;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Source of truth + final backstop against overselling. Stateless: caller passes
 * the {@link Connection} so one instance can be shared.
 *
 * <p>Claims are grouped by opportunity (Kafka is partitioned by opportunity_id)
 * and each group is settled with ONE bulk-upsert statement on its own
 * connection/transaction, which keeps {@code remaining} accurate and never
 * oversells: lock the opportunity row (FOR UPDATE), drop already-booked
 * drivers (idempotency), admit at most {@code remaining} new drivers in arrival
 * order, insert them, decrement {@code remaining} by exactly the number
 * inserted. Each statement touches a single opportunity row, so groups for
 * different opportunities can be settled concurrently on separate connections
 * without deadlocking.
 *
 * Schema (see com/tm/infra/migrations/0001_init.sql):
 * <pre>
 *   opportunities(opportunity_id PK, region_id, zone_id, booking_window_start,
 *                 booking_window_end, capacity INT, remaining INT)
 *   bookings(booking_id BIGSERIAL PK, opportunity_id, driver_id, idempotency_key,
 *            status, created_at, UNIQUE(opportunity_id, driver_id))
 * </pre>
 *
 * <p>region_id/zone_id are descriptive metadata from the delivery-opportunity
 * domain model and are not read by settleOpportunity. booking_window_start/end
 * is normally enforced earlier by the Redis gate
 * ({@code com.tm.common.redis.ClaimGate#claim}); settleOpportunity also checks
 * it as a backstop (treats out-of-window as zero remaining capacity) in case
 * the gate's window check was bypassed.
 */
public interface ClaimStore {

    /** COMMITTED = new booking; DUPLICATE = already booked (idempotent); REJECTED = capacity full or booking window closed. */
    enum Outcome { COMMITTED, DUPLICATE, REJECTED }

    /**
     * Settles all claims for ONE opportunity with a single bulk statement on
     * {@code conn} (caller owns the transaction). All events must share the same
     * {@code opportunityId}. Returns the outcome per event, in the same order as
     * {@code events}.
     */
    List<Outcome> settleOpportunity(Connection conn, String opportunityId, List<ClaimEvent> events)
            throws SQLException;
}
