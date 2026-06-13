-- Source of truth + backstop for the booking-claim flow (see CLAUDE.md).
-- region_id/zone_id/booking_window_* come from the delivery-opportunity domain
-- model: region_id/zone_id are descriptive (filtering/reporting only, not used
-- by the claim path); booking_window_start/end are enforced by the Redis gate
-- (ClaimGate.claim) via Redis' own clock, so PG just stores them as the source
-- of truth for what the gate was configured with.
CREATE TABLE opportunities (
    opportunity_id        TEXT PRIMARY KEY,
    region_id             TEXT NOT NULL,
    zone_id               TEXT NOT NULL,
    booking_window_start  TIMESTAMPTZ NOT NULL,
    booking_window_end    TIMESTAMPTZ NOT NULL,
    capacity              INT NOT NULL,
    remaining             INT NOT NULL
);

CREATE TABLE bookings (
    booking_id       BIGSERIAL PRIMARY KEY,
    -- ON DELETE CASCADE: deleting an opportunity drops its bookings too, so the
    -- DELETE /opportunities/{id} admin op doesn't trip the FK (handler also clears
    -- opp_meta + claimed_set in Redis). Booking history is expendable on opp delete.
    opportunity_id   TEXT NOT NULL REFERENCES opportunities (opportunity_id) ON DELETE CASCADE,
    driver_id        TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    status           TEXT NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (opportunity_id, driver_id)
);
