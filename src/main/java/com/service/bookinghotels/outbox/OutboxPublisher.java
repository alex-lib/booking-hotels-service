package com.service.bookinghotels.outbox;

public interface OutboxPublisher {

    void publish(OutboxRow row) throws Exception;
}
