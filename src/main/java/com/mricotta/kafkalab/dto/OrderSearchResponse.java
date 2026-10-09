package com.mricotta.kafkalab.dto;

import java.util.List;

public record OrderSearchResponse(long total, int page, int size, List<OrderSearchResult> results) {
}
