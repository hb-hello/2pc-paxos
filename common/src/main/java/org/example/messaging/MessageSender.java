package org.example.messaging;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageSender {

    private static final Logger logger = LogManager.getLogger(MessageSender.class);

    protected final int nodeId;
    private boolean isPrimaryServer;
    protected final StubManager stubManager;
    private final AtomicBoolean active;

    private final ExecutorService networkExecutor;

    public MessageSender(int nodeId, ExecutorService networkExecutor) {
        this.nodeId = nodeId;
        this.networkExecutor = networkExecutor;
        this.stubManager = new StubManager(nodeId, networkExecutor);
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
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}