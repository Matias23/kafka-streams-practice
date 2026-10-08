package com.mricotta.kafkalab.exception;

/** Permanent failure: retrying will not fix it, the record goes straight to the dead letter topic. */
public class InvalidOrderException extends RuntimeException {

    public InvalidOrderException(String message) {
        super(message);
    }
}
