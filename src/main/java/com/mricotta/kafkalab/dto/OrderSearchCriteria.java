package com.mricotta.kafkalab.dto;

import java.math.BigDecimal;

/** Search filters; every filter is optional and null means "do not filter by this". */
public record OrderSearchCriteria(
        String text,
        String customerId,
        BigDecimal minAmount,
        BigDecimal maxAmount,
        int page,
        int size) {

    public boolean hasText() {
        return text != null && !text.isBlank();
    }
}
