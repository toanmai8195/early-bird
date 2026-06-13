package com.tm.common.redis;

import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;

import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

/** Non-blocking {@link PgHealth} backed by the Vert.x Redis client. */
@Singleton
public final class VertxPgHealth implements PgHealth {

    private final RedisAPI redis;

    @Inject
    public VertxPgHealth(RedisAPI redis) {
        this.redis = redis;
    }

    @Override
    public Future<Void> markDown() {
        // SET pg_health down EX <ttl>: the TTL self-heals the flag if the manager
        // that set it dies before clearing it.
        return redis.set(List.of(KEY, DOWN, "EX", Long.toString(DOWN_TTL_SECONDS))).mapEmpty();
    }

    @Override
    public Future<Void> markUp() {
        return redis.del(List.of(KEY)).mapEmpty();
    }

    @Override
    public Future<Boolean> isHealthy() {
        return redis.exists(List.of(KEY)).map(resp -> resp.toInteger() == 0);
    }
}
