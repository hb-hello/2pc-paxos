package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;

public class ExecutorManager {

    private static final Logger logger = LogManager.getLogger(ExecutorManager.class);

    // Best practice: 2x CPU cores for mixed I/O and CPU-bound tasks
    private static final int MAX_THREADS = Runtime.getRuntime().availableProcessors() * 2;
    private static final int QUEUE_CAPACITY = 65_536; // 64K

    private final ExecutorService stateExecutor;
    private final ExecutorService stateMachineExecutor;
    private final ExecutorService networkExecutor;
    private final ExecutorService messageExecutor;
    private final ExecutorService grpcExecutor;
    private final ExecutorService listeningExecutor;
    private final ScheduledExecutorService timerExecutor;
    private final ScheduledExecutorService retryExecutor;



    public ExecutorManager() {

        // State management: Single-threaded to avoid race conditions on state mutations
        this.stateExecutor = Executors.newSingleThreadExecutor(createNamedThreadFactory("state-manager"));

        // Log management: Single-threaded to maintain sequential consistency of log entries
        this.stateMachineExecutor = Executors.newSingleThreadExecutor(createNamedThreadFactory("state-machine"));

        // Network I/O: Fixed thread pool sized for concurrent network operations
        // Size based on: otherServerCount * 2 (for send/receive) + buffer
//        this.networkExecutor = createMonitoredNetworkExecutor();
        this.networkExecutor = Executors.newFixedThreadPool(
                100,
                createNamedThreadFactory("network-io")
        );

        // Message processing: Fixed thread pool with bounded queue to prevent resource exhaustion
        // CallerRunsPolicy provides backpressure when queue is full
        this.messageExecutor = new ThreadPoolExecutor(
                MAX_THREADS,
                MAX_THREADS,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(QUEUE_CAPACITY),
                createNamedThreadFactory("message-processor"),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // gRPC executor: Bounded configuration for gRPC server/channel operations
        this.grpcExecutor = new ThreadPoolExecutor(
                MAX_THREADS,
                MAX_THREADS,
                60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(QUEUE_CAPACITY),
                createNamedThreadFactory("grpc-executor"),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // Executor for listening to incoming messages - for use with grpc server
        this.listeningExecutor = Executors.newSingleThreadExecutor(createNamedThreadFactory("grpc-listener"));

        this.timerExecutor = Executors.newScheduledThreadPool(4, createNamedThreadFactory("timer-executor"));

        this.retryExecutor = Executors.newScheduledThreadPool(4, createNamedThreadFactory("retry-manager"));

    }

    public ExecutorService getStateExecutor() {
        return stateExecutor;
    }

    public ExecutorService getNetworkExecutor() {
        return networkExecutor;
    }

    public ExecutorService getStateMachineExecutor() {
        return stateMachineExecutor;
    }

    public ScheduledExecutorService getTimerExecutor() {
        return timerExecutor;
    }

    public ScheduledExecutorService getRetryExecutor() {
        return retryExecutor;
    }

    public ExecutorService getGrpcExecutor() {
        return grpcExecutor;
    }

    public ExecutorService getMessageExecutor() {
        return messageExecutor;
    }

    public void submitStateTransition(Runnable task) {
        stateExecutor.submit(task);
    }

    public void submitLogOperation(Runnable task) {
        stateMachineExecutor.submit(task);
    }

    public Future<?> submitNetworkIO(Runnable task) {
        return networkExecutor.submit(task);
    }

    public void submitMessageProcessing(Runnable task) {
        messageExecutor.submit(task);
    }

    public void submitListeningTask(Runnable task) {
        listeningExecutor.submit(task);
    }

    public void shutdown() {
        shutdownExecutor(networkExecutor, "Network");
        shutdownExecutor(messageExecutor, "Message");
        shutdownExecutor(grpcExecutor, "gRPC");
        shutdownExecutor(stateMachineExecutor, "Log");
        shutdownExecutor(stateExecutor, "State");
        shutdownExecutor(listeningExecutor, "Listening");
    }

    private void shutdownExecutor(ExecutorService executor, String name) {
        logger.info("Shutting down {} executor", name);
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("{} executor did not terminate in time, forcing shutdown", name);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.error("{} executor shutdown interrupted", name);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Creates a ThreadFactory that produces named threads for better debugging
     * and monitoring. Named threads help identify thread pool types in thread dumps.
     */
    private ThreadFactory createNamedThreadFactory(String poolName) {
        return new ThreadFactory() {
            private int counter = 0;

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("-" + poolName + "-" + counter++);
                thread.setDaemon(false); // Ensure proper shutdown control
                return thread;
            }
        };
    }
}
