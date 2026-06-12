package com.tm.services.manager.handler;

import com.tm.common.kafka.ClaimEvent;

import java.util.List;

/** Consumer-side handler for a batch of claim events. */
public interface ClaimHandler {

    /** Throws {@link com.tm.common.exception.HandlerException} on failure. */
    void handleBatch(List<ClaimEvent> events);
}
