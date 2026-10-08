package com.mricotta.kafkalab.service;

import com.mricotta.kafkalab.event.OrderEvent;
import com.mricotta.kafkalab.exception.InvalidOrderException;
import com.mricotta.kafkalab.exception.OrderProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OrderProcessingService {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessingService.class);

    public void process(OrderEvent event) {
        // simulated failures for the lab: the product decides how processing fails
        switch (event.product()) {
            case "invalid" -> throw new InvalidOrderException("Invalid order orderId=" + event.orderId());
            case "fail" -> {
                log.warn("ATTEMPT FAILED orderId={}", event.orderId());
                throw new OrderProcessingException("Transient failure orderId=" + event.orderId());
            }
            default -> log.info("PROCESSED orderId={} customerId={} amount={}",
                    event.orderId(), event.customerId(), event.amount());
        }
    }
}
