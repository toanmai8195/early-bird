package com.tm.services.server.handler;

import com.tm.common.metric.Metrics;
import com.tm.services.server.dao.Opportunity;
import com.tm.services.server.dao.OpportunityDao;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Handles the opportunity CRUD endpoints, delegating to {@link OpportunityDao}
 * (PG source of truth, syncs {@code opp_meta:{opp}} in Redis for the claim-path
 * gate).
 *
 * <p>See .claude/rules/observability-metrics.md — each endpoint records a
 * counter tagged by outcome + a P99 latency timer.
 */
@Singleton
public final class OpportunityHandlerImpl implements OpportunityHandler {

    private static final String METRIC = "booking.api.opportunity";
    private static final String LATENCY = "booking.api.opportunity.latency";

    private final OpportunityDao opportunities;
    private final Metrics metrics;

    @Inject
    public OpportunityHandlerImpl(OpportunityDao opportunities, Metrics metrics) {
        this.opportunities = opportunities;
        this.metrics = metrics;
    }

    @Override
    public void create(RoutingContext ctx) {
        Timer.Sample sample = Timer.start();
        Opportunity request;
        try {
            request = Opportunity.fromRequest(ctx.pathParam("id"), ctx.body().asJsonObject());
        } catch (IllegalArgumentException e) {
            respondError(ctx, sample, 400, "invalid", e);
            return;
        }
        opportunities.create(request)
                .onComplete(ar -> {
                    if (ar.succeeded()) {
                        respond(ctx, sample, 201, "created", ar.result().toJson());
                    } else {
                        respondError(ctx, sample, 409, "error", ar.cause());
                    }
                });
    }

    @Override
    public void get(RoutingContext ctx) {
        Timer.Sample sample = Timer.start();
        opportunities.get(ctx.pathParam("id"))
                .onComplete(ar -> {
                    if (ar.failed()) {
                        respondError(ctx, sample, 503, "error", ar.cause());
                    } else {
                        respondOptional(ctx, sample, 200, "found", ar.result());
                    }
                });
    }

    @Override
    public void update(RoutingContext ctx) {
        Timer.Sample sample = Timer.start();
        String opportunityId = ctx.pathParam("id");
        Opportunity request;
        try {
            request = Opportunity.fromRequest(opportunityId, ctx.body().asJsonObject());
        } catch (IllegalArgumentException e) {
            respondError(ctx, sample, 400, "invalid", e);
            return;
        }
        opportunities.update(opportunityId, request)
                .onComplete(ar -> {
                    if (ar.failed()) {
                        respondError(ctx, sample, 503, "error", ar.cause());
                    } else {
                        respondOptional(ctx, sample, 200, "updated", ar.result());
                    }
                });
    }

    @Override
    public void delete(RoutingContext ctx) {
        Timer.Sample sample = Timer.start();
        opportunities.delete(ctx.pathParam("id"))
                .onComplete(ar -> {
                    if (ar.failed()) {
                        respondError(ctx, sample, 503, "error", ar.cause());
                    } else if (ar.result()) {
                        respond(ctx, sample, 204, "deleted", null);
                    } else {
                        respond(ctx, sample, 404, "not_found", new JsonObject().put("error", "not found"));
                    }
                });
    }

    private void respondOptional(RoutingContext ctx, Timer.Sample sample, int foundStatus, String foundResult,
                                  Optional<Opportunity> opportunity) {
        if (opportunity.isPresent()) {
            respond(ctx, sample, foundStatus, foundResult, opportunity.get().toJson());
        } else {
            respond(ctx, sample, 404, "not_found", new JsonObject().put("error", "not found"));
        }
    }

    private void respondError(RoutingContext ctx, Timer.Sample sample, int status, String result, Throwable cause) {
        respond(ctx, sample, status, result, new JsonObject().put("error", cause.getMessage()));
    }

    private void respond(RoutingContext ctx, Timer.Sample sample, int status, String result, JsonObject body) {
        // CRUD is setup traffic, not a claim scenario: caller from X-Caller-Id if the
        // client sets it, else "system" so it's distinguishable from scenario traffic.
        String caller = ctx.request().getHeader("X-Caller-Id");
        Tags tags = Tags.of("result", result, "caller", caller == null || caller.isBlank() ? "system" : caller);
        metrics.counter(METRIC, tags).increment();
        sample.stop(metrics.timer(LATENCY, tags));
        ctx.response().setStatusCode(status);
        if (body == null) {
            ctx.response().end();
        } else {
            ctx.response().putHeader("content-type", "application/json").end(body.encode());
        }
    }
}
