package com.mricotta.kafkalab.controller;

import com.mricotta.kafkalab.dto.CustomerTotalResponse;
import com.mricotta.kafkalab.service.CustomerTotalService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/customers")
public class CustomerController {

    private final CustomerTotalService customerTotalService;

    public CustomerController(CustomerTotalService customerTotalService) {
        this.customerTotalService = customerTotalService;
    }

    @GetMapping("/{customerId}/total")
    public CustomerTotalResponse getTotal(@PathVariable String customerId) {
        return customerTotalService.getTotal(customerId);
    }
}
