package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;

public class ExecutorManager {

    private static final Logger logger = LogManager.getLogger(ExecutorManager.class);

    private final ExecutorService stateExecutor;
    private final ExecutorService logExecutor;
    private final ExecutorService networkExecutor;
    private final ExecutorService streamingExecutor;
    private final ExecutorService messageExecutor;
    private final ExecutorService listeningExecutor;



    public ExecutorManager(int otherServerCount) {

        // State management: Single-threaded to avoid race conditions on state mutations
        this.stateExecutor = Executors.newSingleThreadExecutor(createNamedThreadFactory("state-manager"));

        // Log management: Single-threaded to maintain sequential consistency of log entries
        this.logExecutor = Executors.newSingleThreadExecutor(createNamedThreadFactory("log-manager"));

        // Network I/O: Fixed thread pool sized for concurrent network operations
        // Size based on: otherServerCount * 2 (for send/receive) + buffer
//        this.networkExecutor = createMonitoredNetworkExecutor();
        this.networkExecutor = Executors.newFixedThreadPool(
                100,
                createNamedThreadFactory("network-io")
        );

        // Streaming operations: Fixed thread pool for long-running streaming RPCs
        // Sized for concurrent NewView operations to all servers
        this.streamingExecutor = Executors.newFixedThreadPool(
                Math.max(5, otherServerCount),
                createNamedThreadFactory("streaming-io")
        );

        // Message processing: Cached thread pool that can scale with incoming message load
        this.messageExecutor = Executors.newCachedThreadPool(createNamedThreadFactory("message-processor"));

        // Executor for listening to incoming messages - for use with grpc server
        this.listeningExecutor = Executors.newSingleThreadExecutor(createNamedThreadFactory("grpc-listener"));

    }

    public ExecutorService getStateExecutor() {
        return stateExecutor;
    }

    public ExecutorService getNetworkExecutor() {
        return networkExecutor;
    }

    public void submitStateTransition(Runnable task) {
        stateExecutor.submit(task);
    }

    public void submitLogOperation(Runnable task) {
        logExecutor.submit(task);
    }

    public Future<?> submitNetworkIO(Runnable task) {
        return networkExecutor.submit(task);
    }

    public void submitStreamingIO(Runnable task) {
        streamingExecutor.submit(task);
    }

    public void submitMessageProcessing(Runnable task) {
        messageExecutor.submit(task);
    }

    public Future<?> submitListeningTask(Runnable task) {
        return listeningExecutor.submit(task);
    }

    public void shutdown() {
        shutdownExecutor(networkExecutor, "Network");
        shutdownExecutor(streamingExecutor, "Streaming");
        shutdownExecutor(messageExecutor, "Message");
        shutdownExecutor(logExecutor, "Log");
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
