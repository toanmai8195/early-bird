package com.tm.services.manager;

import com.tm.common.kafka.ClaimEvent;
import com.tm.common.kafka.ClaimProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Consumes claim events from Kafka and commits them to Postgres. Also runs the
 * reconciliation job that scans the Redis pending set for stuck entries and
 * either replays them or records failures for follow-up.
 */
public final class Main {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv().getOrDefault("KAFKA_BROKERS", "localhost:9092"));
        props.put("group.id", "claim-manager");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");

        ClaimProcessor processor = new ClaimProcessor(null); // TODO: wire ClaimStore (PG).

        System.out.println("manager (claim processor) starting");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(ClaimProducer.TOPIC));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> rec : records) {
                    // TODO: deserialize rec.value() -> ClaimEvent, then processor.handle(event).
                    ClaimEvent event = parse(rec.value());
                    try {
                        processor.handle(event);
                    } catch (Exception e) {
                        System.err.println("commit failed, will reconcile: " + e.getMessage());
                    }
                }
                consumer.commitSync();
            }
        }
    }

    private static ClaimEvent parse(String json) {
        // TODO: real JSON parsing.
        return new ClaimEvent("", "", "");
    }
}
