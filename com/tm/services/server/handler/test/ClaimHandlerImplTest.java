package com.tm.services.server.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.tm.common.kafka.ClaimProducer;
import com.tm.common.metric.Metrics;
import com.tm.common.metric.MicrometerMetrics;
import com.tm.common.redis.ClaimGate;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ClaimHandlerImplTest {

    private ClaimGate gate;
    private ClaimProducer producer;
    private SimpleMeterRegistry registry;
    private Metrics metrics;
    private CircuitBreaker circuitBreaker;
    private ClaimHandlerImpl handler;

    private RoutingContext ctx;
    private HttpServerRequest request;
    private HttpServerResponse response;

    @Before
    public void setUp() {
        gate = mock(ClaimGate.class);
        producer = mock(ClaimProducer.class);
        registry = new SimpleMeterRegistry();
        metrics = new MicrometerMetrics(registry);
        circuitBreaker = CircuitBreaker.of("test-gate", CircuitBreakerConfig.ofDefaults());
        handler = new ClaimHandlerImpl(gate, producer, metrics, circuitBreaker);

        ctx = mock(RoutingContext.class);
        request = mock(HttpServerRequest.class);
        response = mock(HttpServerResponse.class);
        when(ctx.pathParam("id")).thenReturn("opp-1");
        when(ctx.request()).thenReturn(request);
        when(ctx.response()).thenReturn(response);
        when(request.getHeader("X-Driver-Id")).thenReturn("driver-1");
        when(request.getHeader("X-Idempotency-Key")).thenReturn("idem-1");
        when(response.setStatusCode(Mockito.anyInt())).thenReturn(response);
        when(response.putHeader(Mockito.anyString(), Mockito.anyString())).thenReturn(response);
        when(producer.publish(any())).thenReturn(Future.succeededFuture());
    }

    @Test
    public void okClaimPublishesAndReturns202() {
        when(gate.claim("opp-1", "driver-1")).thenReturn(Future.succeededFuture(ClaimGate.Result.OK));

        handler.handle(ctx);

        Mockito.verify(producer).publish(any());
        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        Mockito.verify(response).setStatusCode(202);
        Mockito.verify(response).end(body.capture());
        assertTrue(body.getValue().contains("ACCEPTED"));
        assertCounter("ok", 1.0);
    }

    @Test
    public void missingDriverIdReturns400WithoutCallingGate() {
        when(request.getHeader("X-Driver-Id")).thenReturn(null);

        handler.handle(ctx);

        Mockito.verify(gate, Mockito.never()).claim(any(), any());
        Mockito.verify(response).setStatusCode(400);
        assertCounter("bad_request", 1.0);
    }

    @Test
    public void missingIdempotencyKeyReturns400WithoutCallingGate() {
        when(request.getHeader("X-Idempotency-Key")).thenReturn(null);

        handler.handle(ctx);

        Mockito.verify(gate, Mockito.never()).claim(any(), any());
        Mockito.verify(response).setStatusCode(400);
        assertCounter("bad_request", 1.0);
    }

    @Test
    public void dupClaimReturns200WithoutPublishing() {
        when(gate.claim("opp-1", "driver-1")).thenReturn(Future.succeededFuture(ClaimGate.Result.DUP));

        handler.handle(ctx);

        Mockito.verify(producer, Mockito.never()).publish(any());
        Mockito.verify(response).setStatusCode(200);
        assertCounter("dup", 1.0);
    }

    @Test
    public void fullClaimReturns409() {
        when(gate.claim("opp-1", "driver-1")).thenReturn(Future.succeededFuture(ClaimGate.Result.FULL));

        handler.handle(ctx);

        Mockito.verify(response).setStatusCode(409);
        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        Mockito.verify(response).end(body.capture());
        assertTrue(body.getValue().contains("FULL"));
        assertCounter("full", 1.0);
    }

    @Test
    public void closedClaimReturns409() {
        when(gate.claim("opp-1", "driver-1")).thenReturn(Future.succeededFuture(ClaimGate.Result.CLOSED));

        handler.handle(ctx);

        Mockito.verify(response).setStatusCode(409);
        ArgumentCaptor<String> body = ArgumentCaptor.forClass(String.class);
        Mockito.verify(response).end(body.capture());
        assertTrue(body.getValue().contains("CLOSED"));
        assertCounter("closed", 1.0);
    }

    @Test
    public void circuitOpenReturns503WithoutCallingGateOrProducer() {
        circuitBreaker.transitionToOpenState();

        handler.handle(ctx);

        Mockito.verify(gate, Mockito.never()).claim(any(), any());
        Mockito.verify(producer, Mockito.never()).publish(any());
        Mockito.verify(response).setStatusCode(503);
        assertCounter("throttled", 1.0);
    }

    private void assertCounter(String result, double expected) {
        double actual = registry.get("booking.api.claim").tag("result", result).counter().count();
        assertEquals(expected, actual, 0.0001);
    }
}
