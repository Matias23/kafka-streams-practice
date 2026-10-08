package com.mricotta.kafkalab.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String ORDERS = "orders";
    public static final String ORDERS_DLT = "orders-dlt";
    public static final String ORDERS_HIGH_VALUE = "orders-high-value";
    public static final String CUSTOMER_TOTALS = "customer-totals";

    @Bean
    public NewTopic ordersTopic() {
        return TopicBuilder.name(ORDERS).partitions(3).replicas(1).build();
    }

    // same partition count as the source topic: the recoverer publishes to the partition the record came from
    @Bean
    public NewTopic ordersDltTopic() {
        return TopicBuilder.name(ORDERS_DLT).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic ordersHighValueTopic() {
        return TopicBuilder.name(ORDERS_HIGH_VALUE).partitions(3).replicas(1).build();
    }

    // compacted: the topic holds state, so Kafka only needs to keep the latest value per key
    @Bean
    public NewTopic customerTotalsTopic() {
        return TopicBuilder.name(CUSTOMER_TOTALS).partitions(3).replicas(1).compact().build();
    }
}
