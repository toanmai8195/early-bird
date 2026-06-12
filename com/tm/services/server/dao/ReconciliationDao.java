package com.tm.services.server.dao;

import io.vertx.core.Future;

import java.util.List;

/**
 * Read-only PG queries used by {@link com.tm.services.server.redis.RedisWarmupService}
 * to rebuild {@code opp_meta:{opp}} / {@code claimed_set:{opp}} in Redis after
 * Redis loses its dataset (e.g. a restart) — PG is the source of truth.
 */
public interface ReconciliationDao {

    /** Opportunities whose booking window is currently open (now() BETWEEN start AND end). */
    Future<List<Opportunity>> listOpenOpportunities();

    /** driver_ids already booked for this opportunity, to rebuild claimed_set:{opp}. */
    Future<List<String>> claimedDrivers(String opportunityId);
}
