package org.example.benchmark;

import org.example.ClientRequest;
import org.example.config.Config;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.ThreadLocalRandom;

public class ClientBenchmark implements ClientMetricsListener {

    private final CountDownLatch latch;

    private final LongAdder completed = new LongAdder();
    private final AtomicLong totalLatencyMillis = new AtomicLong(0L);
    private final AtomicLong minLatencyMillis = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxLatencyMillis = new AtomicLong(0L);

    private volatile long startTimeNanos;
    private volatile long endTimeNanos;

    public ClientBenchmark(int totalRequests) {
        this.latch = new CountDownLatch(totalRequests);
    }

    public void start() {
        this.startTimeNanos = System.nanoTime();
    }

    public void awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        latch.await(timeout, unit);
        this.endTimeNanos = System.nanoTime();
    }

    @Override
    public void onRequestCompleted(ClientRequest request, long latencyMillis) {
        completed.increment();
        totalLatencyMillis.addAndGet(latencyMillis);

        minLatencyMillis.updateAndGet(prev -> Math.min(prev, latencyMillis));
        maxLatencyMillis.updateAndGet(prev -> Math.max(prev, latencyMillis));

        latch.countDown();
    }

    public void printResults() {
        long done = completed.sum();
        if (done == 0) {
            System.out.println("Benchmark: no requests completed.");
            return;
        }

        double avgLatency = totalLatencyMillis.get() / (double) done;
        long min = minLatencyMillis.get() == Long.MAX_VALUE ? 0 : minLatencyMillis.get();
        long max = maxLatencyMillis.get();

        double elapsedSeconds = (endTimeNanos - startTimeNanos) / 1_000_000_000.0;
        double throughput = elapsedSeconds > 0 ? done / elapsedSeconds : 0.0;

        System.out.printf("Benchmark complete: %d requests in %.3f s%n", done, elapsedSeconds);
        System.out.printf("Throughput: %.2f requests/second%n", throughput);
        System.out.printf("Latency (ms) - avg: %.2f, min: %d, max: %d%n",
                avgLatency, min, max);
    }

    /**
     * Build 6000 intra-shard transfer transactions, evenly split across clusters.
     * Uses accountIdToClusterIndex from DBHandler and Config server.cluster.count. [attached_file:1]
     */
    public static List<String> buildIntraShardTransfers(Map<Integer, Integer> accountToClusterIndex,
                                                        int totalRequests) {
        int clusterCount = Config.getServerClusterCount();
        if (totalRequests % clusterCount != 0) {
            throw new IllegalArgumentException("totalRequests must be divisible by clusterCount");
        }

        // Group account IDs by cluster index
        Map<Integer, List<Integer>> accountsByCluster = new HashMap<>();
        for (Map.Entry<Integer, Integer> e : accountToClusterIndex.entrySet()) {
            int accountId = e.getKey();
            int clusterIdx = e.getValue();
            accountsByCluster
                    .computeIfAbsent(clusterIdx, k -> new ArrayList<>())
                    .add(accountId);
        }

        int perCluster = totalRequests / clusterCount;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        List<String> txs = new ArrayList<>(totalRequests);

        for (int clusterIdx = 0; clusterIdx < clusterCount; clusterIdx++) {
            List<Integer> accounts = accountsByCluster.get(clusterIdx);
            if (accounts == null || accounts.size() < 2) {
                throw new IllegalStateException("Not enough accounts in cluster " + clusterIdx);
            }

            for (int i = 0; i < perCluster; i++) {
                int senderIdx = rnd.nextInt(accounts.size());
                int receiverIdx;
                do {
                    receiverIdx = rnd.nextInt(accounts.size());
                } while (receiverIdx == senderIdx);

                int sender = accounts.get(senderIdx);
                int receiver = accounts.get(receiverIdx);

                double amount = rnd.nextDouble(1.0, 10.0);
                // Simple "a,b,amount" format; ClientNode will parse as a transfer. [attached_file:1]
                String tx = sender + "," + receiver + "," + String.format(Locale.US, "%.2f", amount);
                txs.add(tx);
            }
        }

        // Shuffle for random order across shards
        Collections.shuffle(txs, rnd);
        return txs;
    }

    /**
     * Build intra-shard transfers where no sender repeats, up to database size.
     */
    public static List<String> buildNonContentiousIntraShardTransfers(Map<Integer, Integer> accountToClusterIndex,
                                                                      int totalRequests) {
        int clusterCount = Config.getServerClusterCount();
        if (totalRequests % clusterCount != 0) {
            throw new IllegalArgumentException("totalRequests must be divisible by clusterCount");
        }
        int databaseSize = Config.getDatabaseSize();
        if (totalRequests > databaseSize) {
            throw new IllegalArgumentException("totalRequests cannot exceed database size " + databaseSize);
        }

        Map<Integer, List<Integer>> accountsByCluster = new HashMap<>();
        for (Map.Entry<Integer, Integer> e : accountToClusterIndex.entrySet()) {
            accountsByCluster
                    .computeIfAbsent(e.getValue(), k -> new ArrayList<>())
                    .add(e.getKey());
        }

        int perCluster = totalRequests / clusterCount;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        List<String> txs = new ArrayList<>(totalRequests);

        for (int clusterIdx = 0; clusterIdx < clusterCount; clusterIdx++) {
            List<Integer> accounts = accountsByCluster.get(clusterIdx);
            if (accounts == null || accounts.size() < 2) {
                throw new IllegalStateException("Not enough accounts in cluster " + clusterIdx);
            }
            if (perCluster > accounts.size()) {
                throw new IllegalStateException("Cluster " + clusterIdx + " does not have enough unique senders");
            }

            List<Integer> shuffledSenders = new ArrayList<>(accounts);
            Collections.shuffle(shuffledSenders, rnd);

            for (int i = 0; i < perCluster; i++) {
                int sender = shuffledSenders.get(i);
                int receiver;
                do {
                    receiver = accounts.get(rnd.nextInt(accounts.size()));
                } while (receiver == sender);

                double amount = rnd.nextDouble(1.0, 10.0);
                String tx = sender + "," + receiver + "," + String.format(Locale.US, "%.2f", amount);
                txs.add(tx);
            }
        }

        Collections.shuffle(txs, rnd);
        return txs;
    }
}
