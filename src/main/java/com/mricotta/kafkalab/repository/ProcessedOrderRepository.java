package com.mricotta.kafkalab.repository;

import java.time.Duration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ProcessedOrderRepository {

    private static final String KEY_PREFIX = "processed-order:";
    // must cover the longest period in which Kafka could redeliver the same record
    private static final Duration RETENTION = Duration.ofHours(24);

    private final StringRedisTemplate redisTemplate;

    public ProcessedOrderRepository(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Records the order as processed.
     *
     * @return true if this is the first time the order is seen, false if it was already recorded
     */
    public boolean markAsProcessed(String orderId) {
        // SET key value NX EX: atomic "write only if absent", so two consumers can never both get true
        var firstTime = redisTemplate.opsForValue().setIfAbsent(key(orderId), "1", RETENTION);
        return Boolean.TRUE.equals(firstTime);
    }

    public void unmark(String orderId) {
        redisTemplate.delete(key(orderId));
    }

    private static String key(String orderId) {
        return KEY_PREFIX + orderId;
    }
}
