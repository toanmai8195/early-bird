package com.tm.common.kafka;

import io.vertx.core.Future;

/**
 * Publishes claim events to Kafka, keyed by opportunity_id so all claims for one
 * opportunity land on the same partition (enables grouped bulk-settle in the
 * manager). Non-blocking: returns a {@link Future} so the server can publish on
 * the event loop without blocking.
 */
public interface ClaimProducer {

    String TOPIC = "claim-events";

    Future<Void> publish(ClaimEvent event);
}
