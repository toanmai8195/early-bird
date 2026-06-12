package com.tm.common.kafka;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Shared Kafka client config for the server (producer) and manager (consumer). */
public final class KafkaClients {

    private KafkaClients() {}

    /** Producer used by the server to publish {@link ClaimEvent}s, keyed by opportunity_id. */
    public static KafkaProducer<String, String> producer(Vertx vertx, String bootstrapServers) {
        Map<String, String> cfg = new HashMap<>();
        cfg.put("bootstrap.servers", bootstrapServers);
        cfg.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        cfg.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        cfg.put("acks", "all");
        return KafkaProducer.create(vertx, cfg);
    }

    /** Consumer used by the manager to poll claim events; offsets committed manually after settle. */
    public static KafkaConsumer<String, String> consumer(String bootstrapServers, String groupId, int maxPollRecords) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }
}
