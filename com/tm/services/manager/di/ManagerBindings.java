package com.tm.services.manager.di;

import com.tm.common.metric.MetricsServer;
import com.tm.common.metric.PrometheusMetricsServer;
import com.tm.common.pg.ClaimStore;
import com.tm.common.pg.PgClaimStore;
import com.tm.common.redis.ClaimGate;
import com.tm.common.redis.VertxClaimGate;
import com.tm.services.manager.dao.BookingDao;
import com.tm.services.manager.dao.JdbcBookingDao;
import com.tm.services.manager.handler.ClaimHandler;
import com.tm.services.manager.handler.ClaimHandlerImpl;
import dagger.Binds;
import dagger.Module;

import javax.inject.Singleton;

/** Binds interfaces to their implementations (constructor-injected impls). */
@Module
public interface ManagerBindings {

    @Binds
    @Singleton
    ClaimStore claimStore(PgClaimStore impl);

    @Binds
    @Singleton
    ClaimGate claimGate(VertxClaimGate impl);

    @Binds
    @Singleton
    BookingDao bookingDao(JdbcBookingDao impl);

    @Binds
    @Singleton
    ClaimHandler claimHandler(ClaimHandlerImpl impl);

    @Binds
    @Singleton
    MetricsServer metricsServer(PrometheusMetricsServer impl);
}
