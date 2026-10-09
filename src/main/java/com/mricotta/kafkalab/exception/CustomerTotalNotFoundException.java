package com.mricotta.kafkalab.exception;

public class CustomerTotalNotFoundException extends RuntimeException {

    public CustomerTotalNotFoundException(String customerId) {
        super("No total found for customerId=" + customerId);
    }
}
