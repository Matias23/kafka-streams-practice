package com.mricotta.kafkalab.listener;

import com.mricotta.kafkalab.config.KafkaTopicConfig;
import com.mricotta.kafkalab.event.OrderEvent;
import com.mricotta.kafkalab.service.OrderProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderListener {

    private static final Logger log = LoggerFactory.getLogger(OrderListener.class);

    private final OrderProcessingService orderProcessingService;

    public OrderListener(OrderProcessingService orderProcessingService) {
        this.orderProcessingService = orderProcessingService;
    }

    @KafkaListener(topics = KafkaTopicConfig.ORDERS)
    public void onOrder(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
        log.info("CONSUMED key={} partition={} offset={}", record.key(), record.partition(), record.offset());
        orderProcessingService.process(record.value());
        // commit the offset only after processing -> at-least-once
        ack.acknowledge();
    }
}
