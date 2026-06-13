package com.tm.services.server.di;

import com.tm.common.kafka.KafkaClients;
import com.tm.services.server.config.ServerConfig;
import dagger.Module;
import dagger.Provides;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import org.postgresql.ds.PGSimpleDataSource;

import java.time.Duration;
import javax.inject.Singleton;
import javax.sql.DataSource;

/**
 * Dagger bindings for third-party types only. Everything we own (BookingVerticle)
 * is constructor-injected via {@code @Inject}, so it never appears here.
 * {@link ServerConfig} is bound via the component factory.
 */
@Module
public final class ServerModule {

    private ServerModule() {}

    @Provides
    @Singleton
    static Vertx vertx() {
        return Vertx.vertx();
    }

    @Provides
    @Singleton
    static PrometheusMeterRegistry prometheusMeterRegistry() {
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }

    @Provides
    static MeterRegistry meterRegistry(PrometheusMeterRegistry registry) {
        return registry;
    }

    @Provides
    @Singleton
    static RedisAPI redisApi(Vertx vertx, ServerConfig config) {
        RedisOptions opts = new RedisOptions()
                .setConnectionString(config.redisUri())
                // Single hot opp bursts ~2K rps onto the gate; 4 connections with the
                // default 24-deep acquire queue overflow (ConnectionPoolTooBusyException).
                // More connections + a deeper acquire queue absorb the burst. The gate is
                // single-key Lua (~100K ops/s capable), so Redis itself is not the limit.
                .setMaxPoolSize(24)
                .setMaxPoolWaiting(1024)
                .setMaxWaitingHandlers(2048);
        return RedisAPI.api(Redis.createClient(vertx, opts));
    }

    @Provides
    @Singleton
    static KafkaProducer<String, String> kafkaProducer(Vertx vertx, ServerConfig config) {
        return KafkaClients.producer(vertx, config.kafkaBrokers());
    }

    @Provides
    @Singleton
    static DataSource dataSource(ServerConfig config) {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(config.jdbcUrl());
        ds.setUser(config.jdbcUser());
        ds.setPassword(config.jdbcPassword());
        return ds;
    }

    /** Worker pool for blocking JDBC ops in opportunity CRUD (off the event loop). */
    @Provides
    @Singleton
    static WorkerExecutor workerExecutor(Vertx vertx, ServerConfig config) {
        return vertx.createSharedWorkerExecutor("pg-opportunity", config.dbPoolSize());
    }

    /**
     * Trips OPEN when the Redis gate keeps failing (down/timeout), so requests
     * fast-reject (or fall to {@link #redisDegradeLimiter}) instead of queuing
     * behind Redis timeouts.
     */
    @Provides
    @Singleton
    static CircuitBreaker redisCircuitBreaker(ServerConfig config) {
        CircuitBreakerConfig cbConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(100)
                .minimumNumberOfCalls(50)
                .waitDurationInOpenState(Duration.ofSeconds(5))
                .build();
        CircuitBreaker cb = CircuitBreaker.of("redis-gate", cbConfig);
        if (config.disableCircuitBreaker()) {
            cb.transitionToDisabledState();
        }
        return cb;
    }

}
