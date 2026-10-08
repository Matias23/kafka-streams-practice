package com.mricotta.kafkalab.config;

import com.mricotta.kafkalab.event.CustomerTotal;
import com.mricotta.kafkalab.event.OrderEvent;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

// Creates the StreamsBuilder bean and starts/stops the KafkaStreams instance with the application
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    // Serde = serializer + deserializer; Kafka Streams needs both because it reads and writes topics
    @Bean
    public Serde<OrderEvent> orderEventSerde() {
        return new JsonSerde<>(OrderEvent.class).noTypeInfo().ignoreTypeHeaders();
    }

    @Bean
    public Serde<CustomerTotal> customerTotalSerde() {
        return new JsonSerde<>(CustomerTotal.class).noTypeInfo().ignoreTypeHeaders();
    }
}
