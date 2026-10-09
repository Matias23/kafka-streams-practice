package com.mricotta.kafkalab.dto;

import java.math.BigDecimal;

public record CustomerOrderStats(
        String customerId,
        long orderCount,
        BigDecimal totalAmount,
        BigDecimal averageAmount) {
}
