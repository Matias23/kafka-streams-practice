package com.mricotta.kafkalab.repository;

import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.util.NamedValue;
import com.mricotta.kafkalab.dto.CustomerOrderStats;
import com.mricotta.kafkalab.entity.OrderDocument;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchAggregations;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.stereotype.Repository;

@Repository
public class OrderStatsRepository {

    private static final String BY_CUSTOMER = "by_customer";
    private static final String TOTAL_AMOUNT = "total_amount";
    private static final String AVERAGE_AMOUNT = "average_amount";

    private final ElasticsearchOperations elasticsearchOperations;

    public OrderStatsRepository(ElasticsearchOperations elasticsearchOperations) {
        this.elasticsearchOperations = elasticsearchOperations;
    }

    public List<CustomerOrderStats> findTopCustomersBySpend(int limit) {
        // terms: one bucket per distinct customerId (like GROUP BY); needs a keyword field.
        // sum/avg: metrics computed inside each bucket. Buckets are ordered by the sum, highest first
        var byCustomer = Aggregation.of(aggregation -> aggregation
                .terms(terms -> terms
                        .field("customerId")
                        .size(limit)
                        .order(List.of(NamedValue.of(TOTAL_AMOUNT, SortOrder.Desc))))
                .aggregations(TOTAL_AMOUNT, sub -> sub.sum(sum -> sum.field("amount")))
                .aggregations(AVERAGE_AMOUNT, sub -> sub.avg(avg -> avg.field("amount"))));

        var query = NativeQuery.builder()
                // size 0: only the aggregation result is needed, not the documents
                .withMaxResults(0)
                .withAggregation(BY_CUSTOMER, byCustomer)
                .build();

        var hits = elasticsearchOperations.search(query, OrderDocument.class);
        var aggregations = (ElasticsearchAggregations) hits.getAggregations();
        if (aggregations == null) {
            return List.of();
        }
        return aggregations.get(BY_CUSTOMER)
                .aggregation()
                .getAggregate()
                .sterms()
                .buckets()
                .array()
                .stream()
                .map(OrderStatsRepository::toStats)
                .toList();
    }

    private static CustomerOrderStats toStats(StringTermsBucket bucket) {
        return new CustomerOrderStats(
                bucket.key().stringValue(),
                bucket.docCount(),
                toAmount(bucket.aggregations().get(TOTAL_AMOUNT).sum().value()),
                toAmount(bucket.aggregations().get(AVERAGE_AMOUNT).avg().value()));
    }

    private static BigDecimal toAmount(Double value) {
        return BigDecimal.valueOf(value).setScale(2, RoundingMode.HALF_UP);
    }
}
