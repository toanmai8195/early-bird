package com.tm.common.kafka;

import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import javax.inject.Inject;
import javax.inject.Singleton;

/** Non-blocking {@link ClaimProducer} backed by the Vert.x Kafka client. */
@Singleton
public final class VertxClaimProducer implements ClaimProducer {

    private final KafkaProducer<String, String> producer;

    @Inject
    public VertxClaimProducer(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    @Override
    public Future<Void> publish(ClaimEvent event) {
        // key = opportunity_id -> same opportunity lands on one partition/consumer,
        // so the manager can group + bulk-settle a poll per opportunity.
        KafkaProducerRecord<String, String> record =
                KafkaProducerRecord.create(TOPIC, event.opportunityId(), event.toJson());
        return producer.write(record);
    }
}
