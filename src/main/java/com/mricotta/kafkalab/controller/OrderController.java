package com.mricotta.kafkalab.controller;

import com.mricotta.kafkalab.dto.OrderRequest;
import com.mricotta.kafkalab.event.OrderEvent;
import com.mricotta.kafkalab.service.OrderProducerService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducerService orderProducerService;

    public OrderController(OrderProducerService orderProducerService) {
        this.orderProducerService = orderProducerService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public OrderEvent create(@Valid @RequestBody OrderRequest request) {
        return orderProducerService.publish(request);
    }
}
