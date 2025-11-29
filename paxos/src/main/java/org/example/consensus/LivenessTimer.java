package org.example.consensus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class LivenessTimer {
    private static final Logger logger = LogManager.getLogger(LivenessTimer.class);

    private final long timeoutMillis;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService callbackExecutor;  // Separate executor for callbacks
    private final Runnable callback;
    private final AtomicReference<ScheduledFuture<?>> scheduledFutureRef;
    private final AtomicBoolean isRunning;
    private final AtomicBoolean isExecutingCallback;

    public LivenessTimer(long timeoutMillis, Runnable callback) {
        this.timeoutMillis = timeoutMillis;
        this.callback = callback;
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.callbackExecutor = Executors.newSingleThreadExecutor();  // Dedicated thread for callbacks
        this.scheduledFutureRef = new AtomicReference<>();
        this.isRunning = new AtomicBoolean(false);
        this.isExecutingCallback = new AtomicBoolean(false);
    }

    private void start() {
        try {
            isRunning.set(true);
            ScheduledFuture<?> future = scheduler.schedule(() -> {
                // Timer has fired - no longer running
                isRunning.set(false);

                // Submit callback to separate executor
                callbackExecutor.submit(() -> {
                    isExecutingCallback.set(true);
                    try {
                        logger.info("Timer callback executing (isRunning=false)");
                        callback.run();
                    } catch (Exception e) {
                        logger.error("Error in timer callback: {}", e.getMessage());
                    } finally {
                        isExecutingCallback.set(false);
                    }
                });
            }, timeoutMillis, TimeUnit.MILLISECONDS);

            scheduledFutureRef.set(future);
        } catch (Exception e) {
            throw new RuntimeException("Timer errored out : " + e.getMessage());
        }
    }

    public void restart() {
        if (!isExecutingCallback()) {
            stop();
            start();
        }
    }

    public synchronized void stop() {
        ScheduledFuture<?> currentFuture = scheduledFutureRef.get();
        if (currentFuture != null) {
            boolean mayInterrupt = !isExecutingCallback.get();
            currentFuture.cancel(mayInterrupt);
            scheduledFutureRef.set(null);
        }
        isRunning.set(false);
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    public boolean isExecutingCallback() {
        return isExecutingCallback.get();
    }

    public void shutdown() {
        stop();

        // Shutdown scheduler
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Shutdown callback executor
        callbackExecutor.shutdown();
        try {
            if (!callbackExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                callbackExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            callbackExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}