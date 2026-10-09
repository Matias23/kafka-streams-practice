package com.mricotta.kafkalab.service;

import com.mricotta.kafkalab.dto.CustomerOrderStats;
import com.mricotta.kafkalab.dto.OrderSearchCriteria;
import com.mricotta.kafkalab.dto.OrderSearchResponse;
import com.mricotta.kafkalab.mapper.OrderDocumentMapper;
import com.mricotta.kafkalab.repository.OrderQueryRepository;
import com.mricotta.kafkalab.repository.OrderStatsRepository;
import java.util.List;
import org.springframework.stereotype.Service;

@Service
public class OrderSearchService {

    private final OrderQueryRepository orderQueryRepository;
    private final OrderStatsRepository orderStatsRepository;
    private final OrderDocumentMapper orderDocumentMapper;

    public OrderSearchService(OrderQueryRepository orderQueryRepository,
                              OrderStatsRepository orderStatsRepository,
                              OrderDocumentMapper orderDocumentMapper) {
        this.orderQueryRepository = orderQueryRepository;
        this.orderStatsRepository = orderStatsRepository;
        this.orderDocumentMapper = orderDocumentMapper;
    }

    public OrderSearchResponse search(OrderSearchCriteria criteria) {
        var hits = orderQueryRepository.search(criteria);
        var results = hits.getSearchHits().stream()
                .map(hit -> orderDocumentMapper.toSearchResult(hit.getContent(), hit.getScore()))
                .toList();
        // total = documents matching the query, not just the ones in this page
        return new OrderSearchResponse(hits.getTotalHits(), criteria.page(), criteria.size(), results);
    }

    public List<CustomerOrderStats> topCustomersBySpend(int limit) {
        return orderStatsRepository.findTopCustomersBySpend(limit);
    }
}
