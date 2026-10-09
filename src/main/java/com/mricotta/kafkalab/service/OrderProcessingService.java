package com.mricotta.kafkalab.service;

import com.mricotta.kafkalab.event.OrderEvent;
import com.mricotta.kafkalab.exception.InvalidOrderException;
import com.mricotta.kafkalab.exception.OrderProcessingException;
import com.mricotta.kafkalab.repository.ProcessedOrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OrderProcessingService {

    private static final Logger log = LoggerFactory.getLogger(OrderProcessingService.class);

    private final ProcessedOrderRepository processedOrderRepository;

    public OrderProcessingService(ProcessedOrderRepository processedOrderRepository) {
        this.processedOrderRepository = processedOrderRepository;
    }

    public void process(OrderEvent event) {
        // the consumer is at-least-once, so the same order can be delivered more than once
        if (!processedOrderRepository.markAsProcessed(event.orderId())) {
            log.info("DUPLICATE SKIPPED orderId={}", event.orderId());
            return;
        }
        try {
            handle(event);
        } catch (RuntimeException ex) {
            // processing failed: remove the mark so the retry is not mistaken for a duplicate
            processedOrderRepository.unmark(event.orderId());
            throw ex;
        }
    }

    private void handle(OrderEvent event) {
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
