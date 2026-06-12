package com.tm.common.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

public class ClaimProducerTest {

    @Test
    public void publishSendsRoundRobinRecordWithJsonValue() {
        MockProducer<String, String> mock =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        ClaimProducer producer = new ClaimProducer(mock);

        producer.publish(new ClaimEvent("opp-1", "driver-9", "idem-abc"));

        assertEquals(1, mock.history().size());
        ProducerRecord<String, String> rec = mock.history().get(0);
        assertEquals(ClaimProducer.TOPIC, rec.topic());
        assertNull("key must be null for round-robin partitioning", rec.key());
        assertEquals(new ClaimEvent("opp-1", "driver-9", "idem-abc").toJson(), rec.value());
    }
}
