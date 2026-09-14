package com.service.bookinghotels.outbox;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.starter.outbox.OutboxMessage;
import org.starter.outbox.OutboxPublisher;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class KafkaOutboxPublisher implements OutboxPublisher {

    private final KafkaTemplate<String, byte[]> outboxKafkaTemplate;

    @Override
    public void publish(OutboxMessage message) throws Exception {
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>(message.topic(), message.key(), message.payload());
        record.headers().add("message-id", message.id().toString().getBytes(StandardCharsets.UTF_8));
        if (message.type() != null) {
            record.headers().add("message-type", message.type().getBytes(StandardCharsets.UTF_8));
        }
        message.headers().forEach((k, v) ->
                record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));

        outboxKafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
    }
}
