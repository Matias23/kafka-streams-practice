package com.mricotta.kafkalab.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mricotta.kafkalab.dto.CustomerOrderStats;
import com.mricotta.kafkalab.dto.OrderSearchCriteria;
import com.mricotta.kafkalab.dto.OrderSearchResult;
import com.mricotta.kafkalab.entity.OrderDocument;
import com.mricotta.kafkalab.mapper.OrderDocumentMapper;
import com.mricotta.kafkalab.repository.OrderQueryRepository;
import com.mricotta.kafkalab.repository.OrderStatsRepository;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;

@ExtendWith(MockitoExtension.class)
class OrderSearchServiceTest {

    private static final Instant CREATED_AT = Instant.parse("2026-01-01T10:00:00Z");

    @Mock
    private OrderQueryRepository orderQueryRepository;

    @Mock
    private OrderStatsRepository orderStatsRepository;

    @Mock
    private SearchHits<OrderDocument> searchHits;

    @Spy
    private OrderDocumentMapper orderDocumentMapper = new OrderDocumentMapper();

    @InjectMocks
    private OrderSearchService orderSearchService;

    @Test
    void mapsHitsAndReportsTheTotalNumberOfMatches() {
        var criteria = new OrderSearchCriteria("laptop", null, null, null, 0, 1);
        var document = new OrderDocument("o1", "c1", "Laptop gamer", new BigDecimal("1500"), CREATED_AT);
        var hit = hit(document, 1.5f);
        when(orderQueryRepository.search(criteria)).thenReturn(searchHits);
        when(searchHits.getTotalHits()).thenReturn(7L);
        when(searchHits.getSearchHits()).thenReturn(List.of(hit));

        var response = orderSearchService.search(criteria);

        assertThat(response.total()).isEqualTo(7);
        assertThat(response.page()).isZero();
        assertThat(response.size()).isEqualTo(1);
        assertThat(response.results()).containsExactly(
                new OrderSearchResult("o1", "c1", "Laptop gamer", new BigDecimal("1500"), CREATED_AT, 1.5f));
    }

    @Test
    void returnsNoScoreWhenResultsAreNotRankedByRelevance() {
        var criteria = new OrderSearchCriteria(null, "c1", null, null, 0, 10);
        var document = new OrderDocument("o1", "c1", "Laptop gamer", new BigDecimal("1500"), CREATED_AT);
        var hit = hit(document, Float.NaN);
        when(orderQueryRepository.search(criteria)).thenReturn(searchHits);
        when(searchHits.getTotalHits()).thenReturn(1L);
        when(searchHits.getSearchHits()).thenReturn(List.of(hit));

        var response = orderSearchService.search(criteria);

        assertThat(response.results()).singleElement()
                .extracting(OrderSearchResult::score)
                .isNull();
    }

    @Test
    void returnsTopCustomersFromTheStatsRepository() {
        var stats = List.of(
                new CustomerOrderStats("c2", 1, new BigDecimal("3200.00"), new BigDecimal("3200.00")),
                new CustomerOrderStats("c1", 2, new BigDecimal("2300.00"), new BigDecimal("1150.00")));
        when(orderStatsRepository.findTopCustomersBySpend(2)).thenReturn(stats);

        assertThat(orderSearchService.topCustomersBySpend(2)).isEqualTo(stats);
    }

    @SuppressWarnings("unchecked")
    private static SearchHit<OrderDocument> hit(OrderDocument document, float score) {
        SearchHit<OrderDocument> hit = mock(SearchHit.class);
        when(hit.getContent()).thenReturn(document);
        when(hit.getScore()).thenReturn(score);
        return hit;
    }
}
