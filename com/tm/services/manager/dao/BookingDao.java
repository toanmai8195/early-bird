package com.tm.services.manager.dao;

import com.tm.common.kafka.ClaimEvent;
import com.tm.common.pg.ClaimStore;
import io.vertx.core.Future;

import java.util.List;

/** Data-access layer for settling claims. */
public interface BookingDao {

    /**
     * Settles all claims for ONE opportunity asynchronously (on a worker pool, off
     * the caller's thread). All events must share {@code opportunityId}. Returns
     * the outcome per event, in the same order as {@code events}; the future
     * fails with the underlying {@link java.sql.SQLException} on error.
     */
    Future<List<ClaimStore.Outcome>> settleOpportunity(String opportunityId, List<ClaimEvent> events);
}
