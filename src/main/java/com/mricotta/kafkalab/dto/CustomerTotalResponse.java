package com.mricotta.kafkalab.dto;

import java.math.BigDecimal;

public record CustomerTotalResponse(String customerId, long orderCount, BigDecimal totalAmount) {
}
