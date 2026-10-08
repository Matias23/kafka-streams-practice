package com.mricotta.kafkalab.exception;

/** Transient failure (e.g. a dependency is down): retried before the record is sent to the dead letter topic. */
public class OrderProcessingException extends RuntimeException {

    public OrderProcessingException(String message) {
        super(message);
    }
}
