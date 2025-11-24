package org.example.messaging;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.config.Config;
import org.example.config.NodeDetails;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ChannelManager {

    private static final Logger logger = LogManager.getLogger(ChannelManager.class);
    private final Map<Integer, CopyOnWriteArrayList<ManagedChannel>> channelPools;
    private final Map<Integer, NodeDetails> nodes;
    private final int poolSize;
    private final Map<Integer, AtomicInteger> roundRobinIndexes;

    public ChannelManager(int poolSize) {
        this.poolSize = poolSize;
        this.channelPools = new ConcurrentHashMap<>();
        this.roundRobinIndexes = new ConcurrentHashMap<>();
        this.nodes = Config.getNodes();
        for (Integer nodeId : nodes.keySet()) {
            createChannelPool(nodeId);
        }
    }

    public ChannelManager(int poolSize, int excludeNodeId) {
        this.poolSize = poolSize;
        this.channelPools = new ConcurrentHashMap<>();
        this.roundRobinIndexes = new ConcurrentHashMap<>();
        this.nodes = Config.getNodesExcept(excludeNodeId);
        for (Integer nodeId : nodes.keySet()) {
            if (!nodeId.equals(excludeNodeId)) {
                createChannelPool(nodeId);
            }
        }
    }

    private void createChannelPool(Integer nodeId) {
        if (!nodes.containsKey(nodeId)) {
            logger.error("Node ID {} not found in configuration while creating GRPC channel.", nodeId);
            throw new RuntimeException();
        }
        NodeDetails node = nodes.get(nodeId);
        CopyOnWriteArrayList<ManagedChannel> pool = new CopyOnWriteArrayList<>();
        for (int i = 0; i < poolSize; i++) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(node.host(), node.port())
                    .usePlaintext()
                    // Reduce aggressive keepalive pings which can trigger ENHANCE_YOUR_CALM on servers.
                    // Increase keepAliveTime to 5 minutes and disable keepAliveWithoutCalls so pings are sent
                    // only when there are active calls. This prevents "too_many_pings" when many channels exist.
                    .keepAliveTime(300, java.util.concurrent.TimeUnit.SECONDS)
                    .keepAliveTimeout(20, java.util.concurrent.TimeUnit.SECONDS)
                    .keepAliveWithoutCalls(false)
                    .build();
            pool.add(channel);
        }
        channelPools.put(nodeId, pool);
        roundRobinIndexes.put(nodeId, new AtomicInteger(0));
        logger.debug("Initialized pool of {} GRPC channels to node {} at {}:{} (keepAliveTime={}s, keepAliveWithoutCalls={})",
                poolSize, nodeId, node.host(), node.port(), 300, false);
    }

    public ManagedChannel getChannel(Integer nodeId) {
        CopyOnWriteArrayList<ManagedChannel> pool = channelPools.get(nodeId);
        if (pool == null || pool.isEmpty()) {
            logger.error("Channel pool for Node ID {} not found.", nodeId);
            throw new RuntimeException();
        }
        AtomicInteger idx = roundRobinIndexes.get(nodeId);
        int i = Math.abs(idx.getAndIncrement() % poolSize);
        return pool.get(i);
    }

    public Map<Integer, NodeDetails> getNodes() {
        return nodes;
    }

    public Map<Integer, CopyOnWriteArrayList<ManagedChannel>> getChannelPools() {
        return channelPools;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public Map<Integer, AtomicInteger> getRoundRobinIndexes() {
        return roundRobinIndexes;
    }

    public void shutdownChannels() {
        for (CopyOnWriteArrayList<ManagedChannel> pool : channelPools.values()) {
            for (ManagedChannel channel : pool) {
                if (channel != null && !channel.isShutdown()) {
                    channel.shutdown();
                }
            }
        }
        logger.info("All GRPC channels have been shut down.");
    }
}
