package com.tm.services.server.handler;

import io.vertx.ext.web.RoutingContext;

/**
 * Handles the opportunity CRUD endpoints under {@code /opportunities/{id}} —
 * one method per HTTP verb, each wired to its own route in BookingVerticle.
 */
public interface OpportunityHandler {

    /** {@code POST /opportunities/{id}}. */
    void create(RoutingContext ctx);

    /** {@code GET /opportunities/{id}}. */
    void get(RoutingContext ctx);

    /** {@code PUT /opportunities/{id}}. */
    void update(RoutingContext ctx);

    /** {@code DELETE /opportunities/{id}}. */
    void delete(RoutingContext ctx);
}
