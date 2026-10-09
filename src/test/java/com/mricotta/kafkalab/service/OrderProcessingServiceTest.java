package com.mricotta.kafkalab.service;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mricotta.kafkalab.event.OrderEvent;
import com.mricotta.kafkalab.exception.InvalidOrderException;
import com.mricotta.kafkalab.exception.OrderProcessingException;
import com.mricotta.kafkalab.repository.ProcessedOrderRepository;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OrderProcessingServiceTest {

    @Mock
    private ProcessedOrderRepository processedOrderRepository;

    @InjectMocks
    private OrderProcessingService orderProcessingService;

    @Test
    void processesAnOrderSeenForTheFirstTime() {
        when(processedOrderRepository.markAsProcessed("o1")).thenReturn(true);

        assertThatCode(() -> orderProcessingService.process(order("o1", "book"))).doesNotThrowAnyException();

        verify(processedOrderRepository, never()).unmark("o1");
    }

    @Test
    void skipsAnOrderThatWasAlreadyProcessed() {
        when(processedOrderRepository.markAsProcessed("o1")).thenReturn(false);

        // "fail" would throw if the order were processed again
        assertThatCode(() -> orderProcessingService.process(order("o1", "fail"))).doesNotThrowAnyException();

        verify(processedOrderRepository, never()).unmark("o1");
    }

    @Test
    void removesTheMarkWhenProcessingFailsSoTheRetryIsNotSkipped() {
        when(processedOrderRepository.markAsProcessed("o1")).thenReturn(true);

        assertThatThrownBy(() -> orderProcessingService.process(order("o1", "fail")))
                .isInstanceOf(OrderProcessingException.class);

        verify(processedOrderRepository).unmark("o1");
    }

    @Test
    void removesTheMarkWhenTheOrderIsInvalid() {
        when(processedOrderRepository.markAsProcessed("o1")).thenReturn(true);

        assertThatThrownBy(() -> orderProcessingService.process(order("o1", "invalid")))
                .isInstanceOf(InvalidOrderException.class);

        verify(processedOrderRepository).unmark("o1");
    }

    private static OrderEvent order(String orderId, String product) {
        return new OrderEvent(orderId, "c1", product, BigDecimal.TEN, Instant.parse("2026-01-01T10:00:00Z"));
    }
}
