package com.mricotta.kafkalab.service;

import com.mricotta.kafkalab.dto.OrderSearchCriteria;
import com.mricotta.kafkalab.dto.OrderSearchResponse;
import com.mricotta.kafkalab.mapper.OrderDocumentMapper;
import com.mricotta.kafkalab.repository.OrderQueryRepository;
import org.springframework.stereotype.Service;

@Service
public class OrderSearchService {

    private final OrderQueryRepository orderQueryRepository;
    private final OrderDocumentMapper orderDocumentMapper;

    public OrderSearchService(OrderQueryRepository orderQueryRepository,
                              OrderDocumentMapper orderDocumentMapper) {
        this.orderQueryRepository = orderQueryRepository;
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
}
