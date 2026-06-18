package com.tm.services.server.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.tm.common.metric.Metrics;
import com.tm.common.metric.MicrometerMetrics;
import com.tm.services.server.dao.Opportunity;
import com.tm.services.server.dao.OpportunityDao;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class OpportunityHandlerImplTest {

    private static final Opportunity OPP =
            new Opportunity("opp-1", "region-1", "zone-1", 1000, 1000, 100L, 200L);

    private OpportunityDao opportunities;
    private SimpleMeterRegistry registry;
    private Metrics metrics;
    private OpportunityHandlerImpl handler;

    private RoutingContext ctx;
    private RequestBody body;
    private HttpServerResponse response;

    @Before
    public void setUp() {
        opportunities = mock(OpportunityDao.class);
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerMetrics(registry);
        handler = new OpportunityHandlerImpl(opportunities, metrics);

        ctx = mock(RoutingContext.class);
        body = mock(RequestBody.class);
        response = mock(HttpServerResponse.class);
        HttpServerRequest request = mock(HttpServerRequest.class);
        when(ctx.request()).thenReturn(request);
        when(request.getHeader("X-Caller-Id")).thenReturn(null); // no caller → "system"
        when(ctx.pathParam("id")).thenReturn("opp-1");
        when(ctx.body()).thenReturn(body);
        when(body.asJsonObject()).thenReturn(OPP.toJson());
        when(ctx.response()).thenReturn(response);
        when(response.setStatusCode(Mockito.anyInt())).thenReturn(response);
        when(response.putHeader(Mockito.anyString(), Mockito.anyString())).thenReturn(response);
    }

    @Test
    public void createReturns201OnSuccess() {
        when(opportunities.create(any())).thenReturn(Future.succeededFuture(OPP));

        handler.create(ctx);

        Mockito.verify(response).setStatusCode(201);
        assertCounter("created", 1.0);
    }

    @Test
    public void createReturns400OnInvalidBody() {
        when(body.asJsonObject()).thenReturn(new JsonObject().put("region_id", "region-1"));

        handler.create(ctx);

        Mockito.verify(response).setStatusCode(400);
        Mockito.verify(opportunities, Mockito.never()).create(any());
        assertCounter("invalid", 1.0);
    }

    @Test
    public void updateReturns400OnInvalidBody() {
        when(body.asJsonObject()).thenReturn(new JsonObject().put("region_id", "region-1"));

        handler.update(ctx);

        Mockito.verify(response).setStatusCode(400);
        Mockito.verify(opportunities, Mockito.never()).update(any(), any());
        assertCounter("invalid", 1.0);
    }

    @Test
    public void createReturns409OnError() {
        when(opportunities.create(any())).thenReturn(Future.failedFuture(new RuntimeException("dup")));

        handler.create(ctx);

        Mockito.verify(response).setStatusCode(409);
        assertCounter("error", 1.0);
    }

    @Test
    public void getReturns200WhenFound() {
        when(opportunities.get("opp-1")).thenReturn(Future.succeededFuture(Optional.of(OPP)));

        handler.get(ctx);

        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        Mockito.verify(response).setStatusCode(200);
        Mockito.verify(response).end(body.capture());
        assertTrue(body.getValue().contains("opp-1"));
        assertCounter("found", 1.0);
    }

    @Test
    public void getReturns404WhenNotFound() {
        when(opportunities.get("opp-1")).thenReturn(Future.succeededFuture(Optional.empty()));

        handler.get(ctx);

        Mockito.verify(response).setStatusCode(404);
        assertCounter("not_found", 1.0);
    }

    @Test
    public void getReturns503OnError() {
        when(opportunities.get("opp-1")).thenReturn(Future.failedFuture(new RuntimeException("db down")));

        handler.get(ctx);

        Mockito.verify(response).setStatusCode(503);
        assertCounter("error", 1.0);
    }

    @Test
    public void updateReturns200WhenFound() {
        when(opportunities.update(eq("opp-1"), any())).thenReturn(Future.succeededFuture(Optional.of(OPP)));

        handler.update(ctx);

        Mockito.verify(response).setStatusCode(200);
        assertCounter("updated", 1.0);
    }

    @Test
    public void updateReturns404WhenNotFound() {
        when(opportunities.update(eq("opp-1"), any())).thenReturn(Future.succeededFuture(Optional.empty()));

        handler.update(ctx);

        Mockito.verify(response).setStatusCode(404);
        assertCounter("not_found", 1.0);
    }

    @Test
    public void deleteReturns204WhenDeleted() {
        when(opportunities.delete("opp-1")).thenReturn(Future.succeededFuture(true));

        handler.delete(ctx);

        Mockito.verify(response).setStatusCode(204);
        assertCounter("deleted", 1.0);
    }

    @Test
    public void deleteReturns404WhenNotFound() {
        when(opportunities.delete("opp-1")).thenReturn(Future.succeededFuture(false));

        handler.delete(ctx);

        Mockito.verify(response).setStatusCode(404);
        assertCounter("not_found", 1.0);
    }

    @Test
    public void deleteReturns503OnError() {
        when(opportunities.delete("opp-1")).thenReturn(Future.failedFuture(new RuntimeException("db down")));

        handler.delete(ctx);

        Mockito.verify(response).setStatusCode(503);
        assertCounter("error", 1.0);
    }

    private void assertCounter(String result, double expected) {
        double actual = registry.get("booking.api.opportunity").tag("result", result).counter().count();
        assertEquals(expected, actual, 0.0001);
    }
}
