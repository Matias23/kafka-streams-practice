package com.mricotta.kafkalab.config;

import com.mricotta.kafkalab.event.OrderEvent;
import com.mricotta.kafkalab.exception.InvalidOrderException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    private static final long RETRY_INTERVAL_MS = 1_000L;
    private static final long MAX_RETRIES = 2L;

    // Spring Boot wires this bean into the container of every @KafkaListener
    @Bean
    public DefaultErrorHandler errorHandler(
            KafkaTemplate<String, OrderEvent> kafkaTemplate,
            ProducerFactory<String, byte[]> producerFactory) {
        // a record that failed deserialization reaches the recoverer as byte[]: it is republished as-is
        var bytesTemplate = new KafkaTemplate<>(producerFactory,
                Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class));

        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);
        templates.put(Object.class, kafkaTemplate);

        // default destination: <topic>-dlt, same partition as the original record
        var recoverer = new DeadLetterPublishingRecoverer(templates);
        // the recoverer adds headers describing the failure; the stack trace is left out to keep them readable
        recoverer.excludeHeader(DeadLetterPublishingRecoverer.HeaderNames.HeadersToAdd.EX_STACKTRACE);

        // 1 attempt + 2 retries, 1s apart; once exhausted the record goes to the dead letter topic
        var errorHandler = new DefaultErrorHandler(recoverer, new FixedBackOff(RETRY_INTERVAL_MS, MAX_RETRIES));
        errorHandler.addNotRetryableExceptions(InvalidOrderException.class);
        return errorHandler;
    }
}
