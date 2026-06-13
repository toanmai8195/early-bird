package com.tm.services.manager.di;

import com.tm.common.kafka.KafkaClients;
import com.tm.common.metric.Metrics;
import com.tm.common.metric.MicrometerMetrics;
import com.tm.services.manager.config.ManagerConfig;
import dagger.Module;
import dagger.Provides;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.postgresql.ds.PGSimpleDataSource;

import javax.inject.Singleton;
import javax.sql.DataSource;

/**
 * Dagger bindings for third-party types only. Everything we own
 * (BookingDao, ClaimHandler, ConsumerRunner) is constructor-injected via
 * {@code @Inject}, so it never appears here. {@link ManagerConfig} is bound via
 * the component factory.
 */
@Module
public final class ManagerModule {

    private ManagerModule() {}

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
    static Metrics metrics(MeterRegistry registry) {
        return new MicrometerMetrics(registry);
    }

    @Provides
    @Singleton
    static DataSource dataSource(ManagerConfig config) {
        // Each getConnection() opens its own physical connection, so concurrent
        // settles (one per opportunity, see JdbcBookingDao) get distinct
        // connections. A production deployment should front this with a real
        // JDBC pool (e.g. HikariCP) sized to ManagerConfig#dbPoolSize.
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(config.jdbcUrl());
        ds.setUser(config.jdbcUser());
        ds.setPassword(config.jdbcPassword());
        return ds;
    }

    @Provides
    @Singleton
    static Vertx vertx() {
        return Vertx.vertx();
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

    @Provides
    @Singleton
    static RedisAPI redisApi(Vertx vertx, ManagerConfig config) {
        return RedisAPI.api(Redis.createClient(vertx, config.redisUri()));
    }
}
