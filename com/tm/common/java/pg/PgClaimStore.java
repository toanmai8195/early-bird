package com.tm.common.pg;

import com.tm.common.kafka.ClaimEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Postgres/JDBC {@link ClaimStore}. Settles all distinct drivers of one
 * opportunity with ONE bulk-upsert statement (instead of one statement per
 * claim).
 *
 * <p>The bulk statement keeps {@code remaining} accurate and never oversells:
 * it locks the opportunity row (FOR UPDATE), drops already-booked drivers
 * (idempotency), admits at most {@code remaining} new drivers in arrival order,
 * inserts them, and decrements {@code remaining} by exactly the number inserted.
 *
 * <p>Backstop for the booking window too: if {@code now()} is outside
 * {@code [booking_window_start, booking_window_end]}, admittable capacity is
 * treated as 0 for this settle, so new drivers are REJECTED even if the Redis
 * gate's window check was bypassed (e.g. degraded mode).
 */
@Singleton
public final class PgClaimStore implements ClaimStore {

    @Inject
    public PgClaimStore() {}

    @Override
    public List<Outcome> settleOpportunity(Connection conn, String opportunityId, List<ClaimEvent> events)
            throws SQLException {
        // Dedup driver (preserve arrival order), keeping the first idempotency key.
        LinkedHashMap<String, String> driverIdem = new LinkedHashMap<>();
        for (ClaimEvent e : events) {
            driverIdem.putIfAbsent(e.driverId(), e.idempotencyKey());
        }

        Map<String, Outcome> byDriver = new HashMap<>();
        try (PreparedStatement ps = conn.prepareStatement(buildSql(driverIdem.size()))) {
            int p = 1;
            for (Map.Entry<String, String> en : driverIdem.entrySet()) {
                ps.setString(p++, en.getKey());    // driver_id
                ps.setString(p++, en.getValue());  // idempotency_key
            }
            ps.setString(p++, opportunityId); // existing join
            ps.setString(p++, opportunityId); // cap FOR UPDATE
            ps.setString(p++, opportunityId); // insert
            ps.setString(p++, opportunityId); // decrement
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    byDriver.put(rs.getString(1), Outcome.valueOf(rs.getString(2)));
                }
            }
        }

        // Map each event (incl. intra-batch duplicates) to its driver's outcome.
        List<Outcome> result = new ArrayList<>(events.size());
        for (ClaimEvent e : events) {
            result.add(byDriver.get(e.driverId()));
        }
        return result;
    }

    private static String buildSql(int n) {
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < n; i++) {
            values.append(i == 0 ? "" : ",").append("(?,?,").append(i + 1).append(")");
        }
        return """
                WITH input(driver_id, idempotency_key, pos) AS (VALUES %s),
                existing AS (
                  SELECT i.driver_id FROM input i
                  JOIN bookings b ON b.opportunity_id = ? AND b.driver_id = i.driver_id
                ),
                cap AS (
                  SELECT CASE WHEN now() BETWEEN booking_window_start AND booking_window_end
                         THEN remaining ELSE 0 END AS remaining
                  FROM opportunities WHERE opportunity_id = ? FOR UPDATE
                ),
                cand AS (
                  SELECT driver_id, idempotency_key, row_number() OVER (ORDER BY pos) AS rn
                  FROM input WHERE driver_id NOT IN (SELECT driver_id FROM existing)
                ),
                adm AS (
                  SELECT driver_id, idempotency_key FROM cand CROSS JOIN cap WHERE rn <= cap.remaining
                ),
                ins AS (
                  INSERT INTO bookings (opportunity_id, driver_id, idempotency_key, status)
                  SELECT ?, driver_id, idempotency_key, 'CONFIRMED' FROM adm
                  ON CONFLICT (opportunity_id, driver_id) DO NOTHING
                  RETURNING driver_id
                ),
                upd AS (
                  UPDATE opportunities SET remaining = remaining - (SELECT count(*) FROM ins)
                  WHERE opportunity_id = ? RETURNING 1
                )
                SELECT i.driver_id,
                  CASE
                    WHEN i.driver_id IN (SELECT driver_id FROM ins) THEN 'COMMITTED'
                    WHEN i.driver_id IN (SELECT driver_id FROM existing) THEN 'DUPLICATE'
                    ELSE 'REJECTED'
                  END
                FROM (SELECT DISTINCT driver_id FROM input) i
                """.formatted(values);
    }
}
