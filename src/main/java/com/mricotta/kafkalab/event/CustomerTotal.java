package com.mricotta.kafkalab.event;

import java.math.BigDecimal;

public record CustomerTotal(String customerId, long orderCount, BigDecimal totalAmount) {

    public static CustomerTotal empty() {
        return new CustomerTotal(null, 0L, BigDecimal.ZERO);
    }

    public CustomerTotal add(OrderEvent order) {
        return new CustomerTotal(order.customerId(), orderCount + 1, totalAmount.add(order.amount()));
    }
}
