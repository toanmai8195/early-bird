package com.tm.common.pg;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Source of truth and final backstop against overselling.
 *
 * <p>Even though Redis fast-rejects, the manager re-checks here:
 * <ul>
 *   <li>UNIQUE(opportunity_id, driver_id) blocks double-booking on Kafka redelivery.</li>
 *   <li>atomic decrement enforces capacity if the Redis gate was bypassed/degraded.</li>
 * </ul>
 *
 * Schema:
 * <pre>
 *   CREATE TABLE opportunities (
 *     opportunity_id TEXT PRIMARY KEY, capacity INT NOT NULL, remaining INT NOT NULL);
 *   CREATE TABLE bookings (
 *     booking_id BIGSERIAL PRIMARY KEY, opportunity_id TEXT NOT NULL,
 *     driver_id TEXT NOT NULL, idempotency_key TEXT NOT NULL, status TEXT NOT NULL,
 *     created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 *     UNIQUE (opportunity_id, driver_id));
 *   CREATE TABLE failed_bookings (LIKE bookings INCLUDING ALL);
 * </pre>
 */
public final class ClaimStore {

    private final Connection conn;

    public ClaimStore(Connection conn) {
        this.conn = conn;
    }

    /**
     * Atomically decrement remaining and insert the booking. Returns false when
     * capacity is exhausted (no oversell) or the booking already exists (idempotent).
     */
    public boolean commitClaim(String opportunityId, String driverId, String idempotencyKey)
            throws SQLException {
        conn.setAutoCommit(false);
        try {
            int updated;
            try (PreparedStatement dec = conn.prepareStatement(
                    "UPDATE opportunities SET remaining = remaining - 1 " +
                    "WHERE opportunity_id = ? AND remaining > 0")) {
                dec.setString(1, opportunityId);
                updated = dec.executeUpdate();
            }
            if (updated == 0) {
                conn.rollback();
                return false; // capacity exhausted
            }
            try (PreparedStatement ins = conn.prepareStatement(
                    "INSERT INTO bookings (opportunity_id, driver_id, idempotency_key, status) " +
                    "VALUES (?, ?, ?, 'CONFIRMED') ON CONFLICT (opportunity_id, driver_id) DO NOTHING")) {
                ins.setString(1, opportunityId);
                ins.setString(2, driverId);
                ins.setString(3, idempotencyKey);
                ins.executeUpdate();
            }
            conn.commit();
            return true;
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        }
    }
}
