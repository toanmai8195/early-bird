package com.tm.services.manager;

import com.tm.common.kafka.ClaimEvent;
import com.tm.common.metric.MetricsServer;
import com.tm.services.manager.config.ManagerConfig;
import com.tm.services.manager.handler.ClaimHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Owns the Kafka poll loop: poll a batch, settle the whole batch in one
 * transaction via {@link ClaimHandler}, then commit offsets (at-least-once).
 * Offsets are committed only after the batch settles, so a crash replays the
 * batch (handled idempotently downstream). Batch size = max.poll.records.
 */
@Singleton
public final class ConsumerRunner {

    private final KafkaConsumer<String, String> consumer;
    private final ClaimHandler handler;
    private final MetricsServer metricsServer;
    private final ManagerConfig config;

    private volatile boolean running = true;

    @Inject
    public ConsumerRunner(KafkaConsumer<String, String> consumer,
                          ClaimHandler handler,
                          MetricsServer metricsServer,
                          ManagerConfig config) {
        this.consumer = consumer;
        this.handler = handler;
        this.metricsServer = metricsServer;
        this.config = config;
    }

    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            consumer.close();
        }));

        System.out.printf("manager starting: brokers=%s group=%s topic=%s pollTimeoutMs=%d maxPollRecords=%d%n",
                config.kafkaBrokers(), config.groupId(), config.topic(),
                config.pollTimeoutMs(), config.maxPollRecords());

        metricsServer.start(config.metricsPort());
        consumer.subscribe(List.of(config.topic()));
        Duration pollTimeout = Duration.ofMillis(config.pollTimeoutMs());
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
            if (records.isEmpty()) {
                continue;
            }
            List<ClaimEvent> batch = new ArrayList<>(records.count());
            for (ConsumerRecord<String, String> rec : records) {
                batch.add(ClaimEvent.fromJson(rec.value()));
            }
            handler.handleBatch(batch);
            // Commit offsets only after the whole batch is settled.
            consumer.commitSync();
        }
    }
}
