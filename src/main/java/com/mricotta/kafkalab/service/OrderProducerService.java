package com.mricotta.kafkalab.service;

import com.mricotta.kafkalab.config.KafkaTopicConfig;
import com.mricotta.kafkalab.dto.OrderRequest;
import com.mricotta.kafkalab.event.OrderEvent;
import com.mricotta.kafkalab.mapper.OrderMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducerService {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final OrderMapper orderMapper;

    public OrderProducerService(KafkaTemplate<String, OrderEvent> kafkaTemplate, OrderMapper orderMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.orderMapper = orderMapper;
    }

    public OrderEvent publish(OrderRequest request) {
        var event = orderMapper.toEvent(request);
        // key = customerId -> all orders of a customer land in the same partition (ordering guaranteed)
        kafkaTemplate.send(KafkaTopicConfig.ORDERS, event.customerId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish orderId={}", event.orderId(), ex);
                        return;
                    }
                    var metadata = result.getRecordMetadata();
                    log.info("PRODUCED orderId={} key={} partition={} offset={}",
                            event.orderId(), event.customerId(), metadata.partition(), metadata.offset());
                });
        return event;
    }
}
