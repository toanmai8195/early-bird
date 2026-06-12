package com.tm.services.server.dao;

import io.vertx.core.Future;

import java.util.Optional;

/**
 * Data-access layer for opportunity CRUD. PG ({@code opportunities}) is the
 * source of truth; {@code opp_meta:{opp}} in Redis (capacity + window_start,
 * TTL'd to window_end) is kept in sync so the claim-path gate
 * ({@link com.tm.common.redis.ClaimGate#claim}) never has to read PG.
 */
public interface OpportunityDao {

    /** Inserts into PG (remaining = capacity) and writes opp_meta:{opp} to Redis. */
    Future<Opportunity> create(Opportunity opportunity);

    /** Reads from PG; empty if the opportunity doesn't exist. */
    Future<Optional<Opportunity>> get(String opportunityId);

    /**
     * Updates PG and refreshes opp_meta:{opp} in Redis. Empty if the opportunity
     * doesn't exist.
     */
    Future<Optional<Opportunity>> update(String opportunityId, Opportunity opportunity);

    /**
     * Deletes from PG and removes opp_meta:{opp} + claimed_set:{opp} from Redis.
     * Returns {@code false} if the opportunity didn't exist.
     */
    Future<Boolean> delete(String opportunityId);
}
