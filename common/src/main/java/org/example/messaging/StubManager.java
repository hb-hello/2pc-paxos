package org.example.messaging;

import org.example.CLIServiceGrpc;
import org.example.PaxosServiceGrpc;
import org.example.ClientServiceGrpc;
import io.grpc.ManagedChannel;
import com.google.protobuf.Empty;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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

    // For each node, keep a pool of stubs for each service
    private static final Map<Integer, CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceFutureStub>> cliStubs = new ConcurrentHashMap<>();
    private static final Map<Integer, CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceFutureStub>> paxosStubs = new ConcurrentHashMap<>();
    private static final Map<Integer, CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceFutureStub>> clientStubs = new ConcurrentHashMap<>();

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

            for (int i = 0; i < channelManager.getPoolSize(); i++) {
                ManagedChannel channel = channelManager.getChannelPools().get(nodeId).get(i);
                cliStubPool.add(CLIServiceGrpc.newFutureStub(channel));
                paxosStubPool.add(PaxosServiceGrpc.newFutureStub(channel));
                clientStubPool.add(ClientServiceGrpc.newFutureStub(channel));

                // create blocking stubs as well
                cliBlockingPool.add(CLIServiceGrpc.newBlockingStub(channel));
                paxosBlockingPool.add(PaxosServiceGrpc.newBlockingStub(channel));
                clientBlockingPool.add(ClientServiceGrpc.newBlockingStub(channel));
            }
            cliStubs.put(nodeId, cliStubPool);
            paxosStubs.put(nodeId, paxosStubPool);
            clientStubs.put(nodeId, clientStubPool);

            cliBlockingStubs.put(nodeId, cliBlockingPool);
            paxosBlockingStubs.put(nodeId, paxosBlockingPool);
            clientBlockingStubs.put(nodeId, clientBlockingPool);
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
        final long PER_CALL_DEADLINE_SECONDS = 2;
        final long PAUSE_BETWEEN_PINGS_MS = 5; // larger pause between pings to avoid bursts
        final long PAUSE_BETWEEN_NODES_MS = 10; // small pause after finishing a node's pool

        for (Integer nodeId : channelManager.getNodes().keySet()) {
            // Warm up all CLI blocking stubs for this node
            CopyOnWriteArrayList<CLIServiceGrpc.CLIServiceBlockingStub> cliPool = cliBlockingStubs.get(nodeId);
            if (cliPool != null) {
                for (CLIServiceGrpc.CLIServiceBlockingStub stub : cliPool) {
                    try {
                        stub.withDeadlineAfter(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance());
                        logger.debug("Warmup CLI ping successful to node {}", nodeId);
                    } catch (Exception e) {
                        logger.warn("Warmup ping to node {} CLI stub failed: {}", nodeId, e.getMessage());
                    }
                    try {
                        Thread.sleep(PAUSE_BETWEEN_PINGS_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            // Warm up all Paxos blocking stubs for this node
            CopyOnWriteArrayList<PaxosServiceGrpc.PaxosServiceBlockingStub> paxosPool = paxosBlockingStubs.get(nodeId);
            if (paxosPool != null) {
                for (PaxosServiceGrpc.PaxosServiceBlockingStub stub : paxosPool) {
                    try {
                        stub.withDeadlineAfter(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance());
                        logger.debug("Warmup Paxos ping successful to node {}", nodeId);
                    } catch (Exception e) {
                        logger.warn("Warmup ping to node {} Paxos stub failed: {}", nodeId, e.getMessage());
                    }
                    try {
                        Thread.sleep(PAUSE_BETWEEN_PINGS_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            // Warm up all Client blocking stubs for this node
            CopyOnWriteArrayList<ClientServiceGrpc.ClientServiceBlockingStub> clientPool = clientBlockingStubs.get(nodeId);
            if (clientPool != null) {
                for (ClientServiceGrpc.ClientServiceBlockingStub stub : clientPool) {
                    try {
                        stub.withDeadlineAfter(PER_CALL_DEADLINE_SECONDS, TimeUnit.SECONDS)
                                .ping(Empty.getDefaultInstance());
                        logger.debug("Warmup Client ping successful to node {}", nodeId);
                    } catch (Exception e) {
                        logger.warn("Warmup ping to node {} Client stub failed: {}", nodeId, e.getMessage());
                    }
                    try {
                        Thread.sleep(PAUSE_BETWEEN_PINGS_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            // Small pause after finishing a node to spread traffic across time
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
