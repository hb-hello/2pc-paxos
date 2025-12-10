package org.example.messaging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class MessageSender {

    private static final Logger logger = LogManager.getLogger(MessageSender.class);

    protected final int nodeId;
    protected StubManager stubManager;
    private final AtomicBoolean active;

    public MessageSender(int nodeId) {
        this.nodeId = nodeId;
        this.stubManager = new StubManager(nodeId);
        this.active = new AtomicBoolean(true);
    }

    public void setActive(boolean active) {
        this.active.set(active);
    }

    public boolean isActive() {
        return active.get();
    }

    public void ensureActive() {
        if (!isActive()) {
            logger.warn("Node {} is inactive. Cannot send messages.", nodeId);
            throw new IllegalStateException("Node is inactive. Cannot send messages.");
        }
    }

    public void warmup() {
        logger.debug("Waiting for everyone to start up...");
        try {
            Thread.sleep(300);
            stubManager.warmup();
            System.out.println("All nodes are up and running.");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void resetStubManager() {
        stubManager = new StubManager(nodeId);
    }
}