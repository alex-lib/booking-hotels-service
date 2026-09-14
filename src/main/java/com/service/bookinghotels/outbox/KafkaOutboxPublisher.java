package com.service.bookinghotels.outbox;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class KafkaOutboxPublisher implements OutboxPublisher {

    private final KafkaTemplate<String, byte[]> outboxKafkaTemplate;

    @Override
    public void publish(OutboxRow row) throws Exception {
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(row.topic(), row.key(), row.payload());
        record.headers().add("message-id", row.id().toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add("message-type", row.type().getBytes(StandardCharsets.UTF_8));
        outboxKafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
    }
}
