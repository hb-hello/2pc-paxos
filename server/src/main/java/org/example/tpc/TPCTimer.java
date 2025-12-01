package org.example.tpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.ClientRequest;
import org.example.messaging.ServerMessage;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class TPCTimer {

    private static final Logger logger = LogManager.getLogger(TPCTimer.class);

    private final long timeoutMillis;
    private final ScheduledExecutorService scheduler;
    private final Consumer<ServerMessage<ClientRequest>> onTimeout;

    // Key: requestId (e.g., ServerMessage.getMessageId), Value: scheduled task
    private final Map<String, ScheduledFuture<?>> timers = new ConcurrentHashMap<>();

    public TPCTimer(long timeoutMillis,
                    ScheduledExecutorService scheduler,
                    Consumer<ServerMessage<ClientRequest>> onTimeout) {
        this.timeoutMillis = timeoutMillis;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.onTimeout = Objects.requireNonNull(onTimeout, "onTimeout");
    }

    /**
     * Start (or restart) a timer for the given request. If a timer already exists
     * for this request, it is cancelled and replaced.
     */
    public void start(ServerMessage<ClientRequest> requestMessage) {
        String key = requestMessage.getMessageId();

        // Cancel any existing timer for this request
        stop(requestMessage);

        Runnable task = () -> {
            try {
                // Remove from map first to avoid races with stop/start.
                timers.remove(key);
                onTimeout.accept(requestMessage);
            } catch (Exception e) {
                logger.error("Error executing timeout callback for {}", key, e);
            }
        };

        ScheduledFuture<?> future =
                scheduler.schedule(task, timeoutMillis, TimeUnit.MILLISECONDS);
        timers.put(key, future);
    }

    /**
     * Stop the timer for the given request if it exists.
     */
    public void stop(ServerMessage<ClientRequest> requestMessage) {
        String key = requestMessage.getMessageId();
        stop(key);
    }

    public void stop(String requestId) {
        ScheduledFuture<?> future = timers.remove(requestId);
        if (future != null) {
            future.cancel(false);
        }
    }

    /**
     * Optionally: stop all timers and shutdown the scheduler (if owned here).
     * Call from server shutdown hook if needed.
     */
    public void shutdown() {
        for (ScheduledFuture<?> future : timers.values()) {
            future.cancel(false);
        }
        timers.clear();
    }
}
