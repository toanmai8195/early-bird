package com.tm.common.exception;

/**
 * Unchecked wrapper thrown when handling a message/request fails, so callers
 * (e.g. a Kafka poll loop) can react without dealing with checked types and
 * decide whether to retry / not commit the offset.
 */
public final class HandlerException extends RuntimeException {

    public HandlerException(String message, Throwable cause) {
        super(message, cause);
    }
}
