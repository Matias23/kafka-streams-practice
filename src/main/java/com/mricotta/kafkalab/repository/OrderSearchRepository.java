package com.mricotta.kafkalab.repository;

import com.mricotta.kafkalab.entity.OrderDocument;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

// Spring Data generates the implementation: save, findById, count, deleteById, ...
public interface OrderSearchRepository extends ElasticsearchRepository<OrderDocument, String> {
}
