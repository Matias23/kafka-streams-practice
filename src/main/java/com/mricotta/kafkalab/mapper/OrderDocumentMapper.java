package com.mricotta.kafkalab.mapper;

import com.mricotta.kafkalab.dto.OrderSearchResult;
import com.mricotta.kafkalab.entity.OrderDocument;
import com.mricotta.kafkalab.event.OrderEvent;
import org.springframework.stereotype.Component;

@Component
public class OrderDocumentMapper {

    public OrderDocument toDocument(OrderEvent event) {
        return new OrderDocument(
                event.orderId(),
                event.customerId(),
                event.product(),
                event.amount(),
                event.createdAt());
    }

    public OrderSearchResult toSearchResult(OrderDocument document, float score) {
        return new OrderSearchResult(
                document.orderId(),
                document.customerId(),
                document.product(),
                document.amount(),
                document.createdAt(),
                // Elasticsearch reports no score (NaN) when results are sorted by a field
                Float.isNaN(score) ? null : score);
    }
}
