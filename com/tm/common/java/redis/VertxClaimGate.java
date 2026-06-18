package com.tm.common.redis;

import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

/** Non-blocking {@link ClaimGate} backed by the Vert.x Redis client. */
@Singleton
public final class VertxClaimGate implements ClaimGate {

    // KEYS[1]=claimed_set:{opp}, KEYS[2]=opp_meta:{opp} (hash: capacity, window_start;
    // TTL'd to window_end at creation time), KEYS[3]=pg_health (global kill switch),
    // ARGV[1]=driver_id
    private static final String SCRIPT =
            "if redis.call('GET', KEYS[3]) == '" + PgHealth.DOWN + "' then return 'DOWN' end\n" +
            "local meta = redis.call('HMGET', KEYS[2], 'capacity', 'window_start')\n" +
            "if not meta[1] then return 'CLOSED' end\n" +
            "local now = tonumber(redis.call('TIME')[1])\n" +
            "if now < tonumber(meta[2]) then return 'CLOSED' end\n" +
            "if redis.call('SCARD', KEYS[1]) >= tonumber(meta[1]) then return 'FULL' end\n" +
            "if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then return 'DUP' end\n" +
            "redis.call('SADD', KEYS[1], ARGV[1])\n" +
            "return 'OK'";

    // SHA1 of SCRIPT == the digest Redis itself keys EVALSHA by, computed locally so
    // we never round-trip SCRIPT LOAD. The hot claim path sends only this 40-char hash
    // instead of the full script body on every request; on NOSCRIPT (Redis restart /
    // flushed script cache) we fall back to EVAL, which re-caches it server-side.
    private static final String SHA = sha1Hex(SCRIPT);

    private final RedisAPI redis;

    @Inject
    public VertxClaimGate(RedisAPI redis) {
        this.redis = redis;
    }

    @Override
    public Future<Result> claim(String opportunityId, String driverId) {
        String claimedKey = "claimed_set:" + opportunityId;
        String metaKey = "opp_meta:" + opportunityId;
        return redis.evalsha(List.of(SHA, "3", claimedKey, metaKey, PgHealth.KEY, driverId))
                .recover(err -> isNoScript(err)
                        ? redis.eval(List.of(SCRIPT, "3", claimedKey, metaKey, PgHealth.KEY, driverId))
                        : Future.failedFuture(err))
                .map(resp -> Result.valueOf(resp.toString()));
    }

    @Override
    public Future<Void> release(String opportunityId, String driverId) {
        return redis.srem(List.of("claimed_set:" + opportunityId, driverId)).mapEmpty();
    }

    @Override
    public Future<Void> releaseAll(String opportunityId, List<String> driverIds) {
        if (driverIds.isEmpty()) {
            return Future.succeededFuture();
        }
        return redis.srem(sremArgs(opportunityId, driverIds)).mapEmpty();
    }

    @Override
    public Future<Void> rejectAll(String opportunityId, List<String> driverIds) {
        if (driverIds.isEmpty()) {
            return Future.succeededFuture();
        }
        String metaKey = "opp_meta:" + opportunityId;
        return redis.srem(sremArgs(opportunityId, driverIds))
                .compose(v -> redis.hincrby(metaKey, "capacity", "-" + driverIds.size()))
                .mapEmpty();
    }

    /** [claimed_set:{opp}, driver1, driver2, ...] for a variadic SREM. */
    private static List<String> sremArgs(String opportunityId, List<String> driverIds) {
        List<String> args = new ArrayList<>(driverIds.size() + 1);
        args.add("claimed_set:" + opportunityId);
        args.addAll(driverIds);
        return args;
    }

    private static boolean isNoScript(Throwable err) {
        String msg = err.getMessage();
        return msg != null && msg.startsWith("NOSCRIPT");
    }

    private static String sha1Hex(String s) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-1").digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 unavailable", e); // every JVM ships SHA-1
        }
    }
}
