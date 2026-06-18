package com.tm.services.manager.di;

import com.tm.common.kafka.KafkaClients;
import com.tm.common.pg.PgPool;
import com.tm.common.metric.Metrics;
import com.tm.common.metric.MicrometerMetrics;
import com.tm.common.redis.PgHealth;
import com.tm.services.manager.config.ManagerConfig;
import dagger.Module;
import dagger.Provides;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.sql.DataSource;

/**
 * Dagger bindings for third-party types only. Everything we own
 * (BookingDao, ClaimHandler, ConsumerRunner) is constructor-injected via
 * {@code @Inject}, so it never appears here. {@link ManagerConfig},
 * {@link PrometheusMeterRegistry}, {@link Vertx}, and the instance ID are bound
 * via the component factory.
 */
@Module
public final class ManagerModule {

    private ManagerModule() {}

    @Provides
    static MeterRegistry meterRegistry(PrometheusMeterRegistry registry) {
        return registry;
    }

    @Provides
    @Named("settleBatchSize")
    static int settleBatchSize(ManagerConfig config) {
        return config.settleBatchSize();
    }

    @Provides
    @Singleton
    static Metrics metrics(MeterRegistry registry) {
        return new MicrometerMetrics(registry);
    }

    @Provides
    @Singleton
    static DataSource dataSource(ManagerConfig config) {
        // Pooled connections (see PgPool): each settle (one per opportunity, see
        // JdbcBookingDao) borrows and returns a connection instead of opening a
        // fresh socket every time. Sized to dbPoolSize, matching the
        // "pg-claim-store" worker pool so every worker has a connection ready.
        return PgPool.dataSource(config.jdbcUrl(), config.jdbcUser(), config.jdbcPassword(),
                config.dbPoolSize(), "pg-claim-store");
    }

    /** Worker pool for parallel per-opportunity JDBC settles (blocking work off the event loop). */
    @Provides
    @Singleton
    static WorkerExecutor workerExecutor(Vertx vertx, ManagerConfig config) {
        return vertx.createSharedWorkerExecutor("pg-claim-store", config.dbPoolSize());
    }

    @Provides
    @Singleton
    static KafkaConsumer<String, String> kafkaConsumer(ManagerConfig config) {
        return KafkaClients.consumer(config.kafkaBrokers(), config.groupId(), config.maxPollRecords());
    }

    /**
     * Trips OPEN when PG settles keep failing or running slow (&gt;5s), so the consumer
     * fast-fails the batch instead of piling more load onto an unhealthy PG. While OPEN
     * the Kafka offset isn't committed, so events redeliver once the breaker half-opens.
     * Enabled by default; {@code --disable-circuit-breaker} forces it DISABLED.
     *
     * <p>State transitions are mirrored to the Redis {@link PgHealth} kill switch:
     * OPEN marks PG down (the server's gate sheds claims), CLOSED clears it (server
     * resumes). OPEN re-fires on every failed half-open probe, refreshing the flag's
     * TTL while PG stays down.
     */
    @Provides
    @Singleton
    static CircuitBreaker pgCircuitBreaker(ManagerConfig config, PgHealth pgHealth) {
        CircuitBreakerConfig cbConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slowCallRateThreshold(50)
                .slowCallDurationThreshold(Duration.ofSeconds(5))
                .slidingWindowSize(50)
                .minimumNumberOfCalls(5)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .build();
        CircuitBreaker cb = CircuitBreaker.of("pg-settle", cbConfig);
        if (config.disableCircuitBreaker()) {
            cb.transitionToDisabledState();
        }
        cb.getEventPublisher().onStateTransition(event -> {
            CircuitBreaker.State to = event.getStateTransition().getToState();
            Future<Void> flag = switch (to) {
                case OPEN, FORCED_OPEN -> pgHealth.markDown();
                case CLOSED -> pgHealth.markUp();
                default -> null;
            };
            if (flag != null) {
                flag.onFailure(err -> System.err.printf(
                        "pg_health update on breaker %s failed: %s%n", to, err.getMessage()));
            }
        });
        return cb;
    }

    @Provides
    @Singleton
    static RedisAPI redisApi(Vertx vertx, ManagerConfig config) {
        // A full/hot opp settles into a burst of rejectAll/releaseAll (gate cleanup);
        // 4 connections throttle that burst even though each call is now a single
        // batched SREM. Give the pool room and a deep acquire queue to absorb it.
        RedisOptions opts = new RedisOptions()
                .setConnectionString(config.redisUri())
                .setMaxPoolSize(16)
                .setMaxPoolWaiting(1024)
                .setMaxWaitingHandlers(2048);
        return RedisAPI.api(Redis.createClient(vertx, opts));
    }
}
