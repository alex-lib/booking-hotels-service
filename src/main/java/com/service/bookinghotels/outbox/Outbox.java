package com.service.bookinghotels.outbox;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class Outbox {
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public void send(String topic, String key, Object event) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException(
                    "Outbox.send() must be called inside an active transaction");
        }

        byte[] payload;
        try {
            payload = objectMapper.writeValueAsBytes(event);
        } catch (JacksonException e) {
            throw new IllegalArgumentException("Unserializable outbox payload", e);
        }

        jdbc.update("""
                INSERT INTO outbox (id, topic, message_key, message_type, payload)
                VALUES (?, ?, ?, ?, ?)
                """,
                UUID.randomUUID(), topic, key, event.getClass().getSimpleName(), payload);
    }
}
