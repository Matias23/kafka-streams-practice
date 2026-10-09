package com.mricotta.kafkalab.service;

import static org.mockito.Mockito.verify;

import com.mricotta.kafkalab.entity.OrderDocument;
import com.mricotta.kafkalab.event.OrderEvent;
import com.mricotta.kafkalab.mapper.OrderDocumentMapper;
import com.mricotta.kafkalab.repository.OrderSearchRepository;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OrderIndexingServiceTest {

    @Mock
    private OrderSearchRepository orderSearchRepository;

    @Spy
    private OrderDocumentMapper orderDocumentMapper = new OrderDocumentMapper();

    @InjectMocks
    private OrderIndexingService orderIndexingService;

    @Test
    void indexesTheOrderUsingItsIdAsDocumentId() {
        var createdAt = Instant.parse("2026-01-01T10:00:00Z");
        var event = new OrderEvent("o1", "c1", "Laptop gamer", new BigDecimal("1500"), createdAt);

        orderIndexingService.index(event);

        verify(orderSearchRepository)
                .save(new OrderDocument("o1", "c1", "Laptop gamer", new BigDecimal("1500"), createdAt));
    }
}
