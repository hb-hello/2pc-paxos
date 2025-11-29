package org.example.messaging;

import org.example.CLIServiceGrpc;
import org.example.PaxosServiceGrpc;
import org.example.ClientServiceGrpc;
import io.grpc.ManagedChannel;
import com.google.protobuf.Empty;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit; // ...added import for deadlines

public class StubManager {
    private static final Logger logger = LogManager.getLogger(StubManager.class);
    private static volatile ChannelManager channelManager; // As already designed, keyed by Integer nodeId
    private static final int POOL_SIZE = 4;

    private final int serverIdToExclude;
    private final ExecutorService networkExecutor;

    // Future stub pools for each service
    private static final Map<Integer, CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceFutureStub>> cliStubs = new ConcurrentHashMap<>();
    private static final Map<Integer, CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceFutureStub>> paxosStubs = new ConcurrentHashMap<>();
    private static final Map<Integer, CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceFutureStub>> clientStubs = new ConcurrentHashMap<>();

    // Async stub pools for each service
    private static final Map<Integer, CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceStub>> cliAsyncStubs = new ConcurrentHashMap<>();
    private static final Map<Integer, CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceStub>> paxosAsyncStubs = new ConcurrentHashMap<>();
    private static final Map<Integer, CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceStub>> clientAsyncStubs = new ConcurrentHashMap<>();

    // Blocking stub pools for each service
    private static final Map<Integer, CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceBlockingStub>> cliBlockingStubs = new ConcurrentHashMap<>();
    private static final Map<Integer, CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceBlockingStub>> paxosBlockingStubs = new ConcurrentHashMap<>();
    private static final Map<Integer, CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceBlockingStub>> clientBlockingStubs = new ConcurrentHashMap<>();

    public StubManager(int serverIdToExclude, ExecutorService networkExecutor) {
        this.serverIdToExclude = serverIdToExclude;
        this.networkExecutor = networkExecutor;
        initChannelManager();
        preCreateStubs();
    }

    //too many pings?

    private synchronized void initChannelManager() {
        if (channelManager == null)
            channelManager = new ChannelManager(POOL_SIZE, this.serverIdToExclude);
    }

    private void preCreateStubs() {
        for (Integer nodeId : channelManager.getNodes().keySet()) {
            CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceFutureStub> cliStubPool = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceFutureStub> paxosStubPool = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceFutureStub> clientStubPool = new CopyOnWriteArrayList<>();

            CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceBlockingStub> cliBlockingPool = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceBlockingStub> paxosBlockingPool = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceBlockingStub> clientBlockingPool = new CopyOnWriteArrayList<>();

            CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceStub> cliAsyncPool = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceStub> paxosAsyncPool = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceStub> clientAsyncPool = new CopyOnWriteArrayList<>();

            for (int i = 0; i < channelManager.getPoolSize(); i++) {
                ManagedChannel channel = channelManager.getChannelPools().get(nodeId).get(i);
                cliStubPool.add(CLIServiceGrpc.newFutureStub(channel));
                paxosStubPool.add(PaxosServiceGrpc.newFutureStub(channel));
                clientStubPool.add(ClientServiceGrpc.newFutureStub(channel));

                cliBlockingPool.add(CLIServiceGrpc.newBlockingStub(channel));
                paxosBlockingPool.add(PaxosServiceGrpc.newBlockingStub(channel));
                clientBlockingPool.add(ClientServiceGrpc.newBlockingStub(channel));

                cliAsyncPool.add(CLIServiceGrpc.newStub(channel));
                paxosAsyncPool.add(PaxosServiceGrpc.newStub(channel));
                clientAsyncPool.add(ClientServiceGrpc.newStub(channel));
            }
            cliStubs.put(nodeId, cliStubPool);
            paxosStubs.put(nodeId, paxosStubPool);
            clientStubs.put(nodeId, clientStubPool);

            cliBlockingStubs.put(nodeId, cliBlockingPool);
            paxosBlockingStubs.put(nodeId, paxosBlockingPool);
            clientBlockingStubs.put(nodeId, clientBlockingPool);

            cliAsyncStubs.put(nodeId, cliAsyncPool);
            paxosAsyncStubs.put(nodeId, paxosAsyncPool);
            clientAsyncStubs.put(nodeId, clientAsyncPool);
        }
    }

    public CLIServiceGrpc.CLIServiceFutureStub getCLIStub(int nodeId) {
        if (channelManager == null)
            throw new IllegalStateException("ChannelManager not initialized");

        CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceFutureStub> pool = cliStubs.get(nodeId);
        if (pool == null || pool.isEmpty())
            throw new IllegalStateException("No CLI stub pool available for node " + nodeId);

        AtomicInteger rr = channelManager.getRoundRobinIndexes().get(nodeId);
        if (rr == null)
            throw new IllegalStateException("No round-robin index for node " + nodeId);

        int idx = Math.abs(rr.getAndIncrement() % channelManager.getPoolSize());
        return pool.get(idx);
    }
    public PaxosServiceGrpc.PaxosServiceFutureStub getPaxosStub(int nodeId) {
        if (channelManager == null)
            throw new IllegalStateException("ChannelManager not initialized");

        CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceFutureStub> pool = paxosStubs.get(nodeId);
        if (pool == null || pool.isEmpty())
            throw new IllegalStateException("No Paxos stub pool available for node " + nodeId);

        AtomicInteger rr = channelManager.getRoundRobinIndexes().get(nodeId);
        if (rr == null)
            throw new IllegalStateException("No round-robin index for node " + nodeId);

        int idx = Math.abs(rr.getAndIncrement() % channelManager.getPoolSize());
        return pool.get(idx);
    }
    public ClientServiceGrpc.ClientServiceFutureStub getClientStub(int nodeId) {
        if (channelManager == null)
            throw new IllegalStateException("ChannelManager not initialized");

        CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceFutureStub> pool = clientStubs.get(nodeId);
        if (pool == null || pool.isEmpty())
            throw new IllegalStateException("No Client stub pool available for node " + nodeId);

        AtomicInteger rr = channelManager.getRoundRobinIndexes().get(nodeId);
        if (rr == null)
            throw new IllegalStateException("No round-robin index for node " + nodeId);

        int idx = Math.abs(rr.getAndIncrement() % channelManager.getPoolSize());
        return pool.get(idx);
    }

    public CLIServiceGrpc.CLIServiceStub getCLIAsyncStub(int nodeId) {
        if (channelManager == null)
            throw new IllegalStateException("ChannelManager not initialized");

        CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceStub> pool = cliAsyncStubs.get(nodeId);
        if (pool == null || pool.isEmpty())
            throw new IllegalStateException("No CLI async stub pool available for node " + nodeId);

        AtomicInteger rr = channelManager.getRoundRobinIndexes().get(nodeId);
        if (rr == null)
            throw new IllegalStateException("No round-robin index for node " + nodeId);

        int idx = Math.abs(rr.getAndIncrement() % channelManager.getPoolSize());
        return pool.get(idx);
    }

    public PaxosServiceGrpc.PaxosServiceStub getPaxosAsyncStub(int nodeId) {
        if (channelManager == null)
            throw new IllegalStateException("ChannelManager not initialized");

        CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceStub> pool = paxosAsyncStubs.get(nodeId);
        if (pool == null || pool.isEmpty())
            throw new IllegalStateException("No Paxos async stub pool available for node " + nodeId);

        AtomicInteger rr = channelManager.getRoundRobinIndexes().get(nodeId);
        if (rr == null)
            throw new IllegalStateException("No round-robin index for node " + nodeId);

        int idx = Math.abs(rr.getAndIncrement() % channelManager.getPoolSize());
        return pool.get(idx);
    }

    public ClientServiceGrpc.ClientServiceStub getClientAsyncStub(int nodeId) {
        if (channelManager == null)
            throw new IllegalStateException("ChannelManager not initialized");

        CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceStub> pool = clientAsyncStubs.get(nodeId);
        if (pool == null || pool.isEmpty())
            throw new IllegalStateException("No Client async stub pool available for node " + nodeId);

        AtomicInteger rr = channelManager.getRoundRobinIndexes().get(nodeId);
        if (rr == null)
            throw new IllegalStateException("No round-robin index for node " + nodeId);

        int idx = Math.abs(rr.getAndIncrement() % channelManager.getPoolSize());
        return pool.get(idx);
    }

    // Blocking stub accessors
    public CLIServiceGrpc.CLIServiceBlockingStub getCLIBlockingStub(int nodeId) {
        if (channelManager == null)
            throw new IllegalStateException("ChannelManager not initialized");

        CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceBlockingStub> pool = cliBlockingStubs.get(nodeId);
        if (pool == null || pool.isEmpty())
            throw new IllegalStateException("No CLI blocking stub pool available for node " + nodeId);

        AtomicInteger rr = channelManager.getRoundRobinIndexes().get(nodeId);
        if (rr == null)
            throw new IllegalStateException("No round-robin index for node " + nodeId);

        int idx = Math.abs(rr.getAndIncrement() % channelManager.getPoolSize());
        return pool.get(idx);
    }

    public PaxosServiceGrpc.PaxosServiceBlockingStub getPaxosBlockingStub(int nodeId) {
        if (channelManager == null)
            throw new IllegalStateException("ChannelManager not initialized");

        CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceBlockingStub> pool = paxosBlockingStubs.get(nodeId);
        if (pool == null || pool.isEmpty())
            throw new IllegalStateException("No Paxos blocking stub pool available for node " + nodeId);

        AtomicInteger rr = channelManager.getRoundRobinIndexes().get(nodeId);
        if (rr == null)
            throw new IllegalStateException("No round-robin index for node " + nodeId);

        int idx = Math.abs(rr.getAndIncrement() % channelManager.getPoolSize());
        return pool.get(idx);
    }

    public ClientServiceGrpc.ClientServiceBlockingStub getClientBlockingStub(int nodeId) {
        if (channelManager == null)
            throw new IllegalStateException("ChannelManager not initialized");

        CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceBlockingStub> pool = clientBlockingStubs.get(nodeId);
        if (pool == null || pool.isEmpty())
            throw new IllegalStateException("No Client blocking stub pool available for node " + nodeId);

        AtomicInteger rr = channelManager.getRoundRobinIndexes().get(nodeId);
        if (rr == null)
            throw new IllegalStateException("No round-robin index for node " + nodeId);

        int idx = Math.abs(rr.getAndIncrement() % channelManager.getPoolSize());
        return pool.get(idx);
    }

    public void warmup() {
        if (channelManager == null)
            throw new IllegalStateException("ChannelManager not initialized");

        // We'll perform synchronous, short-deadline pings one-by-one to avoid flooding servers with PINGs.
        final long PER_CALL_DEADLINE_SECONDS = 1;
        final long PAUSE_BETWEEN_PINGS_MS = 5; // larger pause between pings to avoid bursts
        final long PAUSE_BETWEEN_NODES_MS = 10; // small pause after finishing a node's pool

        for (Integer nodeId : channelManager.getNodes().keySet()) {
            CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceBlockingStub> cliBlockingPool = cliBlockingStubs.get(nodeId);
            if (cliBlockingPool != null) {
                for (CLIServiceGrpc.CLIServiceBlockingStub stub : cliBlockingPool) {
                    try {
                        stub.withDeadlineAfter(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance());
                        logger.debug("Warmup CLI ping successful to node {}", nodeId);
                    } catch (Exception e) {
                        logger.debug("Warmup ping to node {} CLI stub failed: {}", nodeId, e.getMessage());
                    }
                    try {
                        Thread.sleep(PAUSE_BETWEEN_PINGS_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceFutureStub> cliFuturePool = cliStubs.get(nodeId);
            if (cliFuturePool != null) {
                for (CLIServiceGrpc.CLIServiceFutureStub stub : cliFuturePool) {
                    try {
                        stub.withDeadlineAfter(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance())
                                .get(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS);
                        logger.debug("Warmup CLI future ping successful to node {}", nodeId);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception e) {
                        logger.debug("Warmup ping to node {} CLI future stub failed: {}", nodeId, e.getMessage());
                    }
                    try {
                        Thread.sleep(PAUSE_BETWEEN_PINGS_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceStub> cliAsyncPool = cliAsyncStubs.get(nodeId);
            if (cliAsyncPool != null) {
                for (CLIServiceGrpc.CLIServiceStub stub : cliAsyncPool) {
                    try {
                        CountDownLatch latch = new CountDownLatch(1);
                        final boolean[] success = new boolean[1];
                        final Throwable[] asyncError = new Throwable[1];
                        stub.withDeadlineAfter(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance(), new StreamObserver<>() {
                                    @Override
                                    public void onNext(Empty value) {
                                    }

                                    @Override
                                    public void onError(Throwable t) {
                                        asyncError[0] = t;
                                        latch.countDown();
                                    }

                                    @Override
                                    public void onCompleted() {
                                        success[0] = true;
                                        latch.countDown();
                                    }
                                });
                        if (!latch.await(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)) {
                            logger.debug("Warmup async CLI ping to node {} timed out", nodeId);
                        } else if (success[0]) {
                            logger.debug("Warmup CLI async ping successful to node {}", nodeId);
                        } else if (asyncError[0] != null) {
                            logger.debug("Warmup ping to node {} CLI async stub failed: {}", nodeId, asyncError[0].getMessage());
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception e) {
                        logger.debug("Warmup async ping to node {} CLI stub failed: {}", nodeId, e.getMessage());
                    }
                    try {
                        Thread.sleep(PAUSE_BETWEEN_PINGS_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceBlockingStub> paxosBlockingPool = paxosBlockingStubs.get(nodeId);
            if (paxosBlockingPool != null) {
                for (PaxosServiceGrpc.PaxosServiceBlockingStub stub : paxosBlockingPool) {
                    try {
                        stub.withDeadlineAfter(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance());
                        logger.debug("Warmup Paxos ping successful to node {}", nodeId);
                    } catch (Exception e) {
                        logger.debug("Warmup ping to node {} Paxos stub failed: {}", nodeId, e.getMessage());
                    }
                    try {
                        Thread.sleep(PAUSE_BETWEEN_PINGS_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceFutureStub> paxosFuturePool = paxosStubs.get(nodeId);
            if (paxosFuturePool != null) {
                for (PaxosServiceGrpc.PaxosServiceFutureStub stub : paxosFuturePool) {
                    try {
                        stub.withDeadlineAfter(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance())
                                .get(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS);
                        logger.debug("Warmup Paxos future ping successful to node {}", nodeId);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception e) {
                        logger.debug("Warmup ping to node {} Paxos future stub failed: {}", nodeId, e.getMessage());
                    }
                    try {
                        Thread.sleep(PAUSE_BETWEEN_PINGS_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceStub> paxosAsyncPool = paxosAsyncStubs.get(nodeId);
            if (paxosAsyncPool != null) {
                for (PaxosServiceGrpc.PaxosServiceStub stub : paxosAsyncPool) {
                    try {
                        CountDownLatch latch = new CountDownLatch(1);
                        final boolean[] success = new boolean[1];
                        final Throwable[] asyncError = new Throwable[1];
                        stub.withDeadlineAfter(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance(), new StreamObserver<>() {
                                    @Override
                                    public void onNext(Empty value) {
                                    }

                                    @Override
                                    public void onError(Throwable t) {
                                        asyncError[0] = t;
                                        latch.countDown();
                                    }

                                    @Override
                                    public void onCompleted() {
                                        success[0] = true;
                                        latch.countDown();
                                    }
                                });
                        if (!latch.await(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)) {
                            logger.debug("Warmup async Paxos ping to node {} timed out", nodeId);
                        } else if (success[0]) {
                            logger.debug("Warmup Paxos async ping successful to node {}", nodeId);
                        } else if (asyncError[0] != null) {
                            logger.debug("Warmup ping to node {} Paxos async stub failed: {}", nodeId, asyncError[0].getMessage());
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception e) {
                        logger.debug("Warmup async ping to node {} Paxos stub failed: {}", nodeId, e.getMessage());
                    }
                    try {
                        Thread.sleep(PAUSE_BETWEEN_PINGS_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceBlockingStub> clientBlockingPool = clientBlockingStubs.get(nodeId);
            if (clientBlockingPool != null) {
                for (ClientServiceGrpc.ClientServiceBlockingStub stub : clientBlockingPool) {
                    try {
                        stub.withDeadlineAfter(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance());
                        logger.debug("Warmup Client ping successful to node {}", nodeId);
                    } catch (Exception e) {
                        logger.debug("Warmup ping to node {} Client stub failed: {}", nodeId, e.getMessage());
                    }
                    try {
                        Thread.sleep(PAUSE_BETWEEN_PINGS_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceFutureStub> clientFuturePool = clientStubs.get(nodeId);
            if (clientFuturePool != null) {
                for (ClientServiceGrpc.ClientServiceFutureStub stub : clientFuturePool) {
                    try {
                        stub.withDeadlineAfter(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance())
                                .get(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS);
                        logger.debug("Warmup Client future ping successful to node {}", nodeId);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception e) {
                        logger.debug("Warmup ping to node {} Client future stub failed: {}", nodeId, e.getMessage());
                    }
                    try {
                        Thread.sleep(PAUSE_BETWEEN_PINGS_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceStub> clientAsyncPool = clientAsyncStubs.get(nodeId);
            if (clientAsyncPool != null) {
                for (ClientServiceGrpc.ClientServiceStub stub : clientAsyncPool) {
                    try {
                        CountDownLatch latch = new CountDownLatch(1);
                        final boolean[] success = new boolean[1];
                        final Throwable[] asyncError = new Throwable[1];
                        stub.withDeadlineAfter(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance(), new StreamObserver<>() {
                                    @Override
                                    public void onNext(Empty value) {
                                    }

                                    @Override
                                    public void onError(Throwable t) {
                                        asyncError[0] = t;
                                        latch.countDown();
                                    }

                                    @Override
                                    public void onCompleted() {
                                        success[0] = true;
                                        latch.countDown();
                                    }
                                });
                        if (!latch.await(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)) {
                            logger.debug("Warmup async Client ping to node {} timed out", nodeId);
                        } else if (success[0]) {
                            logger.debug("Warmup Client async ping successful to node {}", nodeId);
                        } else if (asyncError[0] != null) {
                            logger.debug("Warmup ping to node {} Client async stub failed: {}", nodeId, asyncError[0].getMessage());
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception e) {
                        logger.debug("Warmup async ping to node {} Client stub failed: {}", nodeId, e.getMessage());
                    }
                    try {
                        Thread.sleep(PAUSE_BETWEEN_PINGS_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            try {
                Thread.sleep(PAUSE_BETWEEN_NODES_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    // Add future/async stub pools and accessors as needed
}
