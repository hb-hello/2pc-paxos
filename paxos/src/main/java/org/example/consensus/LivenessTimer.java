package org.example.consensus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
    private final AtomicLong runGeneration;
    private final Object lifecycleLock = new Object();

    public LivenessTimer(long timeoutMillis, Runnable callback) {
        this.timeoutMillis = timeoutMillis;
        this.callback = callback;
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.callbackExecutor = Executors.newSingleThreadExecutor();  // Dedicated thread for callbacks
        this.scheduledFutureRef = new AtomicReference<>();
        this.isRunning = new AtomicBoolean(false);
        this.isExecutingCallback = new AtomicBoolean(false);
        this.runGeneration = new AtomicLong(0L);
    }

    private void startLocked(String name) {
        try {
            isRunning.set(true);
            final long runId = runGeneration.incrementAndGet();
            ScheduledFuture<?> future = scheduler.schedule(() -> {
                isRunning.set(false);
                if (runGeneration.get() != runId) {
                    logger.debug("Timer firing skipped for {} due to stale run {}", name, runId);
                    scheduledFutureRef.set(null);
                    return;
                }
                scheduledFutureRef.set(null);
                callbackExecutor.submit(() -> {
                    if (runGeneration.get() != runId) {
                        return;
                    }
                    isExecutingCallback.set(true);
                    try {
                        logger.info("Timer callback executing (isRunning=false) for : {}", name);
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
            isRunning.set(false);
            throw new RuntimeException("Timer errored out : " + e.getMessage());
        }
    }

    public void startIfNotRunning(String name) {
        synchronized (lifecycleLock) {
            if (isRunning.get()) {
                return;
            }
            startLocked(name);
        }
    }

    public void restart(String name) {
        synchronized (lifecycleLock) {
            cancelLocked();
            startLocked(name);
        }
    }

    public void stop() {
        synchronized (lifecycleLock) {
            cancelLocked();
        }
    }

    private void cancelLocked() {
        ScheduledFuture<?> currentFuture = scheduledFutureRef.get();
        if (currentFuture != null) {
            boolean mayInterrupt = !isExecutingCallback.get();
            currentFuture.cancel(mayInterrupt);
            scheduledFutureRef.set(null);
        }
        isRunning.set(false);
        runGeneration.incrementAndGet();
    }

    public boolean isRunning() {
        return isRunning.get();
    }


    @SuppressWarnings("unused")
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