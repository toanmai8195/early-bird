package com.tm.common.redis;

import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;

import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

/** Non-blocking {@link ClaimGate} backed by the Vert.x Redis client. */
@Singleton
public final class VertxClaimGate implements ClaimGate {

    // KEYS[1]=claimed_set:{opp}, KEYS[2]=opp_meta:{opp} (hash: capacity, window_start;
    // TTL'd to window_end at creation time), ARGV[1]=driver_id
    private static final String SCRIPT =
            "local meta = redis.call('HMGET', KEYS[2], 'capacity', 'window_start')\n" +
            "if not meta[1] then return 'CLOSED' end\n" +
            "local now = tonumber(redis.call('TIME')[1])\n" +
            "if now < tonumber(meta[2]) then return 'CLOSED' end\n" +
            "if redis.call('SCARD', KEYS[1]) >= tonumber(meta[1]) then return 'FULL' end\n" +
            "if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then return 'DUP' end\n" +
            "redis.call('SADD', KEYS[1], ARGV[1])\n" +
            "return 'OK'";

    private final RedisAPI redis;

    @Inject
    public VertxClaimGate(RedisAPI redis) {
        this.redis = redis;
    }

    @Override
    public Future<Result> claim(String opportunityId, String driverId) {
        String claimedKey = "claimed_set:" + opportunityId;
        String metaKey = "opp_meta:" + opportunityId;
        return redis.eval(List.of(SCRIPT, "2", claimedKey, metaKey, driverId))
                .map(resp -> Result.valueOf(resp.toString()));
    }

    @Override
    public Future<Void> release(String opportunityId, String driverId) {
        String key = "claimed_set:" + opportunityId;
        return redis.srem(List.of(key, driverId)).mapEmpty();
    }

    @Override
    public Future<Void> reject(String opportunityId, String driverId) {
        String claimedKey = "claimed_set:" + opportunityId;
        String metaKey = "opp_meta:" + opportunityId;
        return redis.srem(List.of(claimedKey, driverId))
                .compose(v -> redis.hincrby(metaKey, "capacity", "-1"))
                .mapEmpty();
    }
}
