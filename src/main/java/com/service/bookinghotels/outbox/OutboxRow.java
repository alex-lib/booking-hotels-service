package com.service.bookinghotels.outbox;
import java.util.UUID;

public record OutboxRow(
        UUID id,
        String topic,
        String key,
        String type,
        byte[] payload,
        int attempts) {
}
