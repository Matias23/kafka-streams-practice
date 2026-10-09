package com.mricotta.kafkalab.service;

import com.mricotta.kafkalab.dto.CustomerTotalResponse;
import com.mricotta.kafkalab.event.CustomerTotal;
import com.mricotta.kafkalab.exception.CustomerTotalNotFoundException;
import com.mricotta.kafkalab.mapper.CustomerTotalMapper;
import com.mricotta.kafkalab.repository.CustomerTotalRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class CustomerTotalService {

    private static final Logger log = LoggerFactory.getLogger(CustomerTotalService.class);

    private final CustomerTotalRepository customerTotalRepository;
    private final CustomerTotalMapper customerTotalMapper;

    public CustomerTotalService(CustomerTotalRepository customerTotalRepository,
                                CustomerTotalMapper customerTotalMapper) {
        this.customerTotalRepository = customerTotalRepository;
        this.customerTotalMapper = customerTotalMapper;
    }

    public void store(CustomerTotal total) {
        customerTotalRepository.save(total);
        log.info("CACHED customerId={} orders={} total={}",
                total.customerId(), total.orderCount(), total.totalAmount());
    }

    public CustomerTotalResponse getTotal(String customerId) {
        return customerTotalRepository.findByCustomerId(customerId)
                .map(customerTotalMapper::toResponse)
                .orElseThrow(() -> new CustomerTotalNotFoundException(customerId));
    }
}
