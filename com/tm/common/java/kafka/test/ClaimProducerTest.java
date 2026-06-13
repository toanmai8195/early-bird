package com.tm.common.kafka;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ClaimProducerTest {

    @Test
    @SuppressWarnings("unchecked")
    public void publishKeysByOpportunityIdWithJsonValue() {
        KafkaProducer<String, String> mock = Mockito.mock(KafkaProducer.class);
        when(mock.write(any())).thenReturn(Future.succeededFuture());

        ClaimProducer producer = new VertxClaimProducer(mock);
        producer.publish(new ClaimEvent("opp-1", "driver-9", "idem-abc", 0L));

        ArgumentCaptor<KafkaProducerRecord<String, String>> rec =
                ArgumentCaptor.forClass(KafkaProducerRecord.class);
        Mockito.verify(mock).write(rec.capture());

        assertEquals(ClaimProducer.TOPIC, rec.getValue().topic());
        assertEquals("opp-1", rec.getValue().key());  // partition by opportunity_id
        assertEquals(new ClaimEvent("opp-1", "driver-9", "idem-abc", 0L).toJson(), rec.getValue().value());
    }
}
