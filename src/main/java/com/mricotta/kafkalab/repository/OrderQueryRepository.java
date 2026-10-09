package com.mricotta.kafkalab.repository;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import com.mricotta.kafkalab.dto.OrderSearchCriteria;
import com.mricotta.kafkalab.entity.OrderDocument;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.stereotype.Repository;

@Repository
public class OrderQueryRepository {

    private static final String PRODUCT = "product";
    private static final String CUSTOMER_ID = "customerId";
    private static final String AMOUNT = "amount";
    private static final String CREATED_AT = "createdAt";

    private final ElasticsearchOperations elasticsearchOperations;

    public OrderQueryRepository(ElasticsearchOperations elasticsearchOperations) {
        this.elasticsearchOperations = elasticsearchOperations;
    }

    public SearchHits<OrderDocument> search(OrderSearchCriteria criteria) {
        var queryBuilder = NativeQuery.builder()
                .withQuery(query -> query.bool(bool -> buildBoolQuery(bool, criteria)))
                // translated to "from" and "size" in the request
                .withPageable(PageRequest.of(criteria.page(), criteria.size()));

        if (!criteria.hasText()) {
            // without text every hit has the same relevance, so order by date instead of by score
            queryBuilder.withSort(Sort.by(Sort.Direction.DESC, CREATED_AT));
        }
        return elasticsearchOperations.search(queryBuilder.build(), OrderDocument.class);
    }

    // A bool query with no clauses matches every document
    private static BoolQuery.Builder buildBoolQuery(BoolQuery.Builder bool, OrderSearchCriteria criteria) {
        if (criteria.hasText()) {
            // must + match: full-text search on the analyzed field; contributes to the relevance score.
            // fuzziness AUTO tolerates small typos ("lapto" still finds "laptop")
            bool.must(must -> must.match(match -> match
                    .field(PRODUCT)
                    .query(criteria.text())
                    .fuzziness("AUTO")));
        }
        if (criteria.customerId() != null) {
            // filter + term: exact yes/no condition on a keyword field; not scored and cacheable
            bool.filter(filter -> filter.term(term -> term
                    .field(CUSTOMER_ID)
                    .value(criteria.customerId())));
        }
        if (criteria.minAmount() != null || criteria.maxAmount() != null) {
            bool.filter(filter -> filter.range(range -> range.number(number -> {
                number.field(AMOUNT);
                if (criteria.minAmount() != null) {
                    number.gte(criteria.minAmount().doubleValue());
                }
                if (criteria.maxAmount() != null) {
                    number.lte(criteria.maxAmount().doubleValue());
                }
                return number;
            })));
        }
        return bool;
    }
}
