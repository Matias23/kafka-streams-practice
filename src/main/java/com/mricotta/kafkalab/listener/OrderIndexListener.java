package com.mricotta.kafkalab.listener;

import com.mricotta.kafkalab.config.KafkaTopicConfig;
import com.mricotta.kafkalab.event.OrderEvent;
import com.mricotta.kafkalab.service.OrderIndexingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderIndexListener {

    private final OrderIndexingService orderIndexingService;

    public OrderIndexListener(OrderIndexingService orderIndexingService) {
        this.orderIndexingService = orderIndexingService;
    }

    // own consumer group: indexing reads every order independently of order processing
    @KafkaListener(topics = KafkaTopicConfig.ORDERS, groupId = "order-indexer")
    public void onOrder(OrderEvent event, Acknowledgment ack) {
        orderIndexingService.index(event);
        ack.acknowledge();
    }
}
