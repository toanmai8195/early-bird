package com.tm.services.server.di;

import com.tm.common.kafka.ClaimProducer;
import com.tm.common.kafka.VertxClaimProducer;
import com.tm.common.metric.Metrics;
import com.tm.common.metric.MetricsServer;
import com.tm.common.metric.MicrometerMetrics;
import com.tm.common.metric.PrometheusMetricsServer;
import com.tm.common.redis.ClaimGate;
import com.tm.common.redis.VertxClaimGate;
import com.tm.services.server.dao.JdbcOpportunityDao;
import com.tm.services.server.dao.JdbcReconciliationDao;
import com.tm.services.server.dao.OpportunityDao;
import com.tm.services.server.dao.ReconciliationDao;
import com.tm.services.server.handler.ClaimHandler;
import com.tm.services.server.handler.ClaimHandlerImpl;
import com.tm.services.server.handler.OpportunityHandler;
import com.tm.services.server.handler.OpportunityHandlerImpl;
import com.tm.services.server.redis.RedisWarmupService;
import com.tm.services.server.redis.RedisWarmupServiceImpl;
import dagger.Binds;
import dagger.Module;

import javax.inject.Singleton;

/** Binds interfaces to their implementations (constructor-injected impls). */
@Module
public interface ServerBindings {

    @Binds
    @Singleton
    ClaimGate claimGate(VertxClaimGate impl);

    @Binds
    @Singleton
    ClaimProducer claimProducer(VertxClaimProducer impl);

    @Binds
    @Singleton
    Metrics metrics(MicrometerMetrics impl);

    @Binds
    @Singleton
    MetricsServer metricsServer(PrometheusMetricsServer impl);

    @Binds
    @Singleton
    OpportunityDao opportunityDao(JdbcOpportunityDao impl);

    @Binds
    @Singleton
    ClaimHandler claimHandler(ClaimHandlerImpl impl);

    @Binds
    @Singleton
    OpportunityHandler opportunityHandler(OpportunityHandlerImpl impl);

    @Binds
    @Singleton
    ReconciliationDao reconciliationDao(JdbcReconciliationDao impl);

    @Binds
    @Singleton
    RedisWarmupService redisWarmupService(RedisWarmupServiceImpl impl);
}
