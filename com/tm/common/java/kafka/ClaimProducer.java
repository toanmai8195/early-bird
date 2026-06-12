package com.tm.common.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Publishes claim events to Kafka (round-robin partitioning, key = null). */
public final class ClaimProducer {

    public static final String TOPIC = "claim-events";

    private final Producer<String, String> producer;

    public ClaimProducer(Producer<String, String> producer) {
        this.producer = producer;
    }

    public void publish(ClaimEvent event) {
        // key = null -> round-robin partitioner spreads load, avoids hot partition.
        producer.send(new ProducerRecord<>(TOPIC, null, event.toJson()));
    }
}
