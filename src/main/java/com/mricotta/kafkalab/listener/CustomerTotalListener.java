package com.mricotta.kafkalab.listener;

import com.mricotta.kafkalab.config.KafkaTopicConfig;
import com.mricotta.kafkalab.event.CustomerTotal;
import com.mricotta.kafkalab.service.CustomerTotalService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class CustomerTotalListener {

    private final CustomerTotalService customerTotalService;

    public CustomerTotalListener(CustomerTotalService customerTotalService) {
        this.customerTotalService = customerTotalService;
    }

    // own consumer group: reads customer-totals independently of every other consumer
    @KafkaListener(
            topics = KafkaTopicConfig.CUSTOMER_TOTALS,
            groupId = "customer-total-cache",
            // the shared consumer config deserializes into OrderEvent; this listener reads another type
            properties = "spring.json.value.default.type=com.mricotta.kafkalab.event.CustomerTotal")
    public void onCustomerTotal(CustomerTotal total, Acknowledgment ack) {
        customerTotalService.store(total);
        ack.acknowledge();
    }
}
