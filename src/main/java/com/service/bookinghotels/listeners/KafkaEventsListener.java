package com.service.bookinghotels.listeners;
import com.service.bookinghotels.entities.statistics.StatisticEvent;
import com.service.bookinghotels.mappers.statistic.StatisticMapper;
import com.service.bookinghotels.services.StatisticService;
import com.service.bookinghotels.web.dto.kafkadto.BookingRoomEvent;
import com.service.bookinghotels.web.dto.kafkadto.RegistrationUserEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;

@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaEventsListener {
    private final StatisticService statisticService;
    private final StatisticMapper statisticMapper;

    @KafkaListener(topics = "${app.kafka.registrationUserTopic}",
            groupId = "${app.kafka.registrationUserGroupId}",
            containerFactory = "kafkaRegistrationUserEventConcurrentKafkaListenerContainerFactory")
    public void listenRegistrationUserEvent(@Payload RegistrationUserEvent event,
                       @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                       @Header(value = "message-id", required = false) byte[] messageId,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp) {
        log.info("Received message: {}", event);
        log.info("Key: {}; Partition: {}; Topic: {}; Timestamp: {}", key, partition, topic, timestamp);
        saveDeduplicated(statisticMapper.registrationUserEventToStatisticEvent(event), messageId);
    }

    @KafkaListener(topics = "${app.kafka.bookingRoomTopic}",
            groupId = "${app.kafka.bookingRoomGroupId}",
            containerFactory = "kafkaBookingRoomEventConcurrentKafkaListenerContainerFactory")
    public void listenBookingRoomEvent(@Payload BookingRoomEvent event,
                       @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                       @Header(value = "message-id", required = false) byte[] messageId,
                       @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp) {
        log.info("Received message: {}", event);
        log.info("Key: {}; Partition: {}; Topic: {}; Timestamp: {}", key, partition, topic, timestamp);
        saveDeduplicated(statisticMapper.bookingRoomEventToStatisticEvent(event), messageId);
    }

    private void saveDeduplicated(StatisticEvent statistic, byte[] messageId) {
        if (messageId != null) {
            statistic.setId(new String(messageId, StandardCharsets.UTF_8));
        }
        statisticService.saveStatistic(statistic);
    }
}
