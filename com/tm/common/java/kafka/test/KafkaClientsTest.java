package com.tm.common.kafka;

import static org.junit.Assert.assertNotNull;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

public class KafkaClientsTest {

    @Test
    public void producerConnectsToConfiguredBrokers() {
        Vertx vertx = Vertx.vertx();
        try {
            KafkaProducer<String, String> producer = KafkaClients.producer(vertx, "localhost:9092");
            assertNotNull(producer);
            producer.close();
        } finally {
            vertx.close();
        }
    }

    @Test
    public void consumerConnectsToConfiguredBrokers() {
        KafkaConsumer<String, String> consumer = KafkaClients.consumer("localhost:9092", "claim-manager", 500);
        try {
            assertNotNull(consumer);
        } finally {
            consumer.close();
        }
    }
}
