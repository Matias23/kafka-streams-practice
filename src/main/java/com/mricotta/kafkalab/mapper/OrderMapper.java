package com.mricotta.kafkalab.mapper;

import com.mricotta.kafkalab.dto.OrderRequest;
import com.mricotta.kafkalab.event.OrderEvent;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class OrderMapper {

    public OrderEvent toEvent(OrderRequest request) {
        return new OrderEvent(
                UUID.randomUUID().toString(),
                request.customerId(),
                request.product(),
                request.amount(),
                Instant.now());
    }
}
