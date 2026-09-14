package com.service.bookinghotels.configuration;
import com.service.bookinghotels.web.dto.kafkadto.BookingRoomEvent;
import com.service.bookinghotels.web.dto.kafkadto.RegistrationUserEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import tools.jackson.databind.json.JsonMapper;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.registrationUserGroupId}")
    private String registrationUserGroupId;

    @Value("${app.kafka.bookingRoomGroupId}")
    private String bookingRoomGroupId;

    @Bean
    public ProducerFactory<String, byte[]> outboxProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, byte[]> outboxKafkaTemplate(ProducerFactory<String, byte[]> outboxProducerFactory) {
        return new KafkaTemplate<>(outboxProducerFactory);
    }


    @Bean
    public ConsumerFactory<String, RegistrationUserEvent> kafkaRegistrationUserEventConsumerFactory(JsonMapper jsonMapper) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, registrationUserGroupId);
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(),
                new JacksonJsonDeserializer<>(RegistrationUserEvent.class, jsonMapper));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RegistrationUserEvent> kafkaRegistrationUserEventConcurrentKafkaListenerContainerFactory(ConsumerFactory<String, RegistrationUserEvent> kafkaRegistrationUserEventConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, RegistrationUserEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaRegistrationUserEventConsumerFactory);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, BookingRoomEvent> kafkaBookingRoomEventConsumerFactory(JsonMapper jsonMapper) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, bookingRoomGroupId);
        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(),
                new JacksonJsonDeserializer<>(BookingRoomEvent.class, jsonMapper));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, BookingRoomEvent> kafkaBookingRoomEventConcurrentKafkaListenerContainerFactory(ConsumerFactory<String, BookingRoomEvent> kafkaBookingRoomEventConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, BookingRoomEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaBookingRoomEventConsumerFactory);
        return factory;
    }
}
