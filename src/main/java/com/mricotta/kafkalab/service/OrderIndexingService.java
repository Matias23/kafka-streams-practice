package com.mricotta.kafkalab.service;

import com.mricotta.kafkalab.event.OrderEvent;
import com.mricotta.kafkalab.mapper.OrderDocumentMapper;
import com.mricotta.kafkalab.repository.OrderSearchRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OrderIndexingService {

    private static final Logger log = LoggerFactory.getLogger(OrderIndexingService.class);

    private final OrderSearchRepository orderSearchRepository;
    private final OrderDocumentMapper orderDocumentMapper;

    public OrderIndexingService(OrderSearchRepository orderSearchRepository,
                                OrderDocumentMapper orderDocumentMapper) {
        this.orderSearchRepository = orderSearchRepository;
        this.orderDocumentMapper = orderDocumentMapper;
    }

    public void index(OrderEvent event) {
        // PUT /orders/_doc/{orderId}: creates the document, or replaces it if the id already exists
        orderSearchRepository.save(orderDocumentMapper.toDocument(event));
        log.info("INDEXED orderId={}", event.orderId());
    }
}
