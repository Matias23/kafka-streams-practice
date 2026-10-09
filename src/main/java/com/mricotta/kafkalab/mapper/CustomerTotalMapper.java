package com.mricotta.kafkalab.mapper;

import com.mricotta.kafkalab.dto.CustomerTotalResponse;
import com.mricotta.kafkalab.event.CustomerTotal;
import org.springframework.stereotype.Component;

@Component
public class CustomerTotalMapper {

    public CustomerTotalResponse toResponse(CustomerTotal total) {
        return new CustomerTotalResponse(total.customerId(), total.orderCount(), total.totalAmount());
    }
}
