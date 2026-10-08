package com.mricotta.kafkalab.event;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderEvent(
        String orderId,
        String customerId,
        String product,
        BigDecimal amount,
        Instant createdAt) {
}
