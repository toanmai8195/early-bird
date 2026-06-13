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
        return RedisAPI.api(Redis.createClient(vertx, config.redisUri()));
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
    static CircuitBreaker redisCircuitBreaker() {
        CircuitBreakerConfig cbConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(20)
                .waitDurationInOpenState(Duration.ofSeconds(5))
                .build();
        return CircuitBreaker.of("redis-gate", cbConfig);
    }

}
