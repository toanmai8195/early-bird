package com.tm.common.redis;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * Atomic early-reject gate. One Lua script per opportunity key
 * (claimed_set:{opp_id}) does counting + dedup in a single atomic op, so the
 * server can fast-reject ~90% of requests once capacity is reached.
 */
public final class ClaimGate {

    /** OK = slot acquired, FULL = capacity reached, DUP = idempotent retry. */
    public enum Result { OK, FULL, DUP }

    // KEYS[1]=claimed_set:{opp}, ARGV[1]=driver_id, ARGV[2]=capacity
    private static final String SCRIPT =
            "if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then return 'DUP' end\n" +
            "if redis.call('SCARD', KEYS[1]) >= tonumber(ARGV[2]) then return 'FULL' end\n" +
            "redis.call('SADD', KEYS[1], ARGV[1])\n" +
            "return 'OK'";

    private final RedisCommands<String, String> redis;

    public ClaimGate(RedisCommands<String, String> redis) {
        this.redis = redis;
    }

    /** Runs the atomic gate for one (opportunity, driver) pair. */
    public Result claim(String opportunityId, String driverId, int capacity) {
        String key = "claimed_set:" + opportunityId;
        String r = redis.eval(SCRIPT, ScriptOutputType.VALUE,
                new String[]{key}, driverId, Integer.toString(capacity));
        return Result.valueOf(r);
    }
}
