package com.mricotta.kafkalab.controller;

import com.mricotta.kafkalab.dto.OrderRequest;
import com.mricotta.kafkalab.dto.OrderSearchCriteria;
import com.mricotta.kafkalab.dto.OrderSearchResponse;
import com.mricotta.kafkalab.event.OrderEvent;
import com.mricotta.kafkalab.service.OrderProducerService;
import com.mricotta.kafkalab.service.OrderSearchService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.math.BigDecimal;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducerService orderProducerService;
    private final OrderSearchService orderSearchService;

    public OrderController(OrderProducerService orderProducerService, OrderSearchService orderSearchService) {
        this.orderProducerService = orderProducerService;
        this.orderSearchService = orderSearchService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public OrderEvent create(@Valid @RequestBody OrderRequest request) {
        return orderProducerService.publish(request);
    }

    @GetMapping("/search")
    public OrderSearchResponse search(
            @RequestParam(required = false) String text,
            @RequestParam(required = false) String customerId,
            @RequestParam(required = false) BigDecimal minAmount,
            @RequestParam(required = false) BigDecimal maxAmount,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "10") @Min(1) @Max(100) int size) {
        return orderSearchService.search(
                new OrderSearchCriteria(text, customerId, minAmount, maxAmount, page, size));
    }
}
