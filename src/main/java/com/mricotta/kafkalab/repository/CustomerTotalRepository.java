package com.mricotta.kafkalab.repository;

import com.mricotta.kafkalab.event.CustomerTotal;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Optional;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class CustomerTotalRepository {

    private static final String KEY_PREFIX = "customer-total:";
    private static final String ORDER_COUNT = "orderCount";
    private static final String TOTAL_AMOUNT = "totalAmount";

    private final HashOperations<String, String, String> hashOperations;

    public CustomerTotalRepository(StringRedisTemplate redisTemplate) {
        this.hashOperations = redisTemplate.opsForHash();
    }

    public void save(CustomerTotal total) {
        // HSET: one Redis hash per customer, one field per attribute; overwrites the previous values
        hashOperations.putAll(key(total.customerId()), Map.of(
                ORDER_COUNT, Long.toString(total.orderCount()),
                TOTAL_AMOUNT, total.totalAmount().toPlainString()));
    }

    public Optional<CustomerTotal> findByCustomerId(String customerId) {
        // HGETALL: returns an empty map when the key does not exist
        var fields = hashOperations.entries(key(customerId));
        if (fields.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new CustomerTotal(
                customerId,
                Long.parseLong(fields.get(ORDER_COUNT)),
                new BigDecimal(fields.get(TOTAL_AMOUNT))));
    }

    private static String key(String customerId) {
        return KEY_PREFIX + customerId;
    }
}
