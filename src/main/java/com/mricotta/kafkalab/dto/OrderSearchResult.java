package com.mricotta.kafkalab.dto;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderSearchResult(
        String orderId,
        String customerId,
        String product,
        BigDecimal amount,
        Instant createdAt,
        // relevance of the match; null when the search has no text and results are sorted by date
        Float score) {
}
