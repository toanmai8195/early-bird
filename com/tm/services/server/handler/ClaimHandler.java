package com.tm.services.server.handler;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;

/** Handles {@code POST /opportunities/{id}/bookings} — the claim endpoint. */
public interface ClaimHandler extends Handler<RoutingContext> {
}
