package com.tm.services.manager;

import com.tm.common.kafka.ClaimEvent;
import com.tm.services.manager.config.ManagerConfig;
import com.tm.services.manager.handler.ClaimHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

/**
 * Owns the Kafka poll loop: poll a batch, settle the whole batch via
 * {@link ClaimHandler}, then commit offsets (at-least-once). Offsets are committed
 * only after the batch settles, so a crash replays the batch (handled idempotently
 * downstream). Batch size = max.poll.records.
 *
 * <p>A batch settle can fail (PG down, or the manager's PG circuit breaker is OPEN
 * and fast-fails the batch). When that happens we must NOT commit the offset and must
 * keep the loop alive: the failed batch is rewound (seek back to its first offset) so
 * Kafka redelivers it, and after a short backoff we poll again. While PG is down the
 * breaker stays OPEN so each redelivery fast-fails cheaply; once {@code PgHealthProbe}
 * closes the breaker the next settle succeeds and the offset advances. Without this
 * catch the exception would escape {@code run()} and kill the (daemon) poll thread,
 * leaving the manager alive but permanently consuming nothing.
 */
@Singleton
public final class ConsumerRunner {

    private final KafkaConsumer<String, String> consumer;
    private final ClaimHandler handler;
    private final ManagerConfig config;
    private final String instanceId;

    private volatile boolean running = true;

    @Inject
    public ConsumerRunner(KafkaConsumer<String, String> consumer,
                          ClaimHandler handler,
                          ManagerConfig config,
                          @Named("instanceId") String instanceId) {
        this.consumer = consumer;
        this.handler = handler;
        this.config = config;
        this.instanceId = instanceId;
    }

    public void run() {
        System.out.printf("manager starting instance=%s brokers=%s group=%s topic=%s pollTimeoutMs=%d maxPollRecords=%d%n",
                instanceId, config.kafkaBrokers(), config.groupId(), config.topic(),
                config.pollTimeoutMs(), config.maxPollRecords());

        consumer.subscribe(List.of(config.topic()));
        Duration pollTimeout = Duration.ofMillis(config.pollTimeoutMs());
        while (running) {
            ConsumerRecords<String, String> records;
            try {
                records = consumer.poll(pollTimeout);
            } catch (WakeupException e) {
                break; // shutdown() called
            }
            if (records.isEmpty()) {
                continue;
            }
            List<ClaimEvent> batch = new ArrayList<>(records.count());
            for (ConsumerRecord<String, String> rec : records) {
                batch.add(ClaimEvent.fromJson(rec.value()));
            }
            try {
                handler.handleBatch(batch);
                // Commit offsets only after the whole batch is settled.
                consumer.commitSync();
            } catch (Exception e) {
                // Settle (or commit) failed — PG down / breaker OPEN. Don't commit;
                // rewind to the batch's first offset so Kafka redelivers it once PG
                // recovers, then back off briefly to avoid hot-looping while down.
                System.err.printf("manager batch failed instance=%s: %s: %s%n",
                        instanceId, e.getClass().getSimpleName(), e.getMessage());
                rewind(records);
                backoff();
            }
        }
        consumer.close(); // close on the poll thread (KafkaConsumer is single-threaded)
        System.out.printf("manager stopped instance=%s%n", instanceId);
    }

    /** Seek each polled partition back to the batch's first offset so it redelivers. */
    private void rewind(ConsumerRecords<String, String> records) {
        try {
            for (TopicPartition tp : records.partitions()) {
                consumer.seek(tp, records.records(tp).get(0).offset());
            }
        } catch (RuntimeException ignored) {
            // Best effort: a rebalance/wakeup may have moved assignment; next poll recovers.
        }
    }

    private void backoff() {
        try {
            Thread.sleep(config.pollTimeoutMs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }

    public void shutdown() {
        running = false;
        consumer.wakeup();
    }
}
