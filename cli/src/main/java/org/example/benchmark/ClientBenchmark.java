package org.example.benchmark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.ClientRequest;
import org.example.config.Config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.ThreadLocalRandom;

public class ClientBenchmark implements ClientMetricsListener {
    private static final Logger logger = LogManager.getLogger(ClientBenchmark.class);

    private final CountDownLatch latch;

    private final LongAdder completed = new LongAdder();
    private final LongAdder aborted = new LongAdder();
    private final LongAdder failed = new LongAdder();

    // Latency accumulators for completed + aborted (non-failed) requests
    private final AtomicLong totalLatencyNonFailedMillis = new AtomicLong(0L);
    private final AtomicLong minLatencyNonFailedMillis = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxLatencyNonFailedMillis = new AtomicLong(0L);

    // Latency accumulators for failed requests (kept separately, not used in throughput/latency)
    private final AtomicLong totalLatencyFailedMillis = new AtomicLong(0L);
    private final AtomicLong minLatencyFailedMillis = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxLatencyFailedMillis = new AtomicLong(0L);

    // Track first and last completion times for non-failed requests (for accurate throughput)
    private final AtomicLong firstNonFailedCompletionNanos = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong lastNonFailedCompletionNanos = new AtomicLong(0L);

    private volatile long startTimeNanos;
    private volatile long endTimeNanos;

    public ClientBenchmark(int totalRequests) {
        this.latch = new CountDownLatch(totalRequests);
    }

    public void start() {
        this.startTimeNanos = System.nanoTime();
    }

    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        boolean completed = latch.await(timeout, unit);
        this.endTimeNanos = System.nanoTime();
        return completed;
    }

    @Override
    public void onRequestCompleted(ClientRequest request, long latencyMillis) {
        completed.increment();
        // Update non-failed latency accumulators
        totalLatencyNonFailedMillis.addAndGet(latencyMillis);
        minLatencyNonFailedMillis.updateAndGet(prev -> Math.min(prev, latencyMillis));
        maxLatencyNonFailedMillis.updateAndGet(prev -> Math.max(prev, latencyMillis));

        // Track first and last completion times for non-failed requests
        firstNonFailedCompletionNanos.updateAndGet(prev -> Math.min(prev, System.nanoTime()));
        lastNonFailedCompletionNanos.set(System.nanoTime());

        latch.countDown();
    }

    @Override
    public void onRequestAborted(ClientRequest request, long latencyMillis) {
        aborted.increment();
        // Aborted requests count as non-failed for throughput/latency
        totalLatencyNonFailedMillis.addAndGet(latencyMillis);
        minLatencyNonFailedMillis.updateAndGet(prev -> Math.min(prev, latencyMillis));
        maxLatencyNonFailedMillis.updateAndGet(prev -> Math.max(prev, latencyMillis));

        // Track first and last completion times for non-failed requests
        firstNonFailedCompletionNanos.updateAndGet(prev -> Math.min(prev, System.nanoTime()));
        lastNonFailedCompletionNanos.set(System.nanoTime());

        latch.countDown();
    }

    @Override
    public void onRequestFailed(ClientRequest request, long latencyMillis) {
        failed.increment();
        // Track failed latencies separately (excluded from throughput/latency stats)
        totalLatencyFailedMillis.addAndGet(latencyMillis);
        minLatencyFailedMillis.updateAndGet(prev -> Math.min(prev, latencyMillis));
        maxLatencyFailedMillis.updateAndGet(prev -> Math.max(prev, latencyMillis));

        latch.countDown();
    }

    public void printResults() {
        long done = completed.sum();
        long abortedCount = aborted.sum();
        long failedCount = failed.sum();
        long total = done + abortedCount + failedCount;

        // For throughput/latency we exclude failed requests
        long nonFailedTotal = done + abortedCount;

        if (total == 0) {
            System.out.println("Benchmark: no requests completed or aborted.");
            return;
        }

        // Compute latency stats over non-failed requests only
        double avgLatency = nonFailedTotal > 0 ? totalLatencyNonFailedMillis.get() / (double) nonFailedTotal : 0.0;
        long min = minLatencyNonFailedMillis.get() == Long.MAX_VALUE ? 0 : minLatencyNonFailedMillis.get();
        long max = maxLatencyNonFailedMillis.get();

        // Calculate elapsed time based on non-failed request window
        // Use time from first non-failed completion to last non-failed completion
        double elapsedSeconds;
        if (nonFailedTotal > 0) {
            long firstCompletion = firstNonFailedCompletionNanos.get();
            long lastCompletion = lastNonFailedCompletionNanos.get();
            if (firstCompletion != Long.MAX_VALUE && lastCompletion > 0) {
                // Use the window from start to last non-failed completion for throughput
                long effectiveEnd = lastCompletion;
                elapsedSeconds = (effectiveEnd - startTimeNanos) / 1_000_000_000.0;
            } else {
                // Fallback to original calculation
                long effectiveEnd = (endTimeNanos == 0L) ? System.nanoTime() : endTimeNanos;
                elapsedSeconds = (effectiveEnd - startTimeNanos) / 1_000_000_000.0;
            }
        } else {
            // No non-failed requests, use full elapsed time
            long effectiveEnd = (endTimeNanos == 0L) ? System.nanoTime() : endTimeNanos;
            elapsedSeconds = (effectiveEnd - startTimeNanos) / 1_000_000_000.0;
        }

        double throughput = elapsedSeconds > 0 ? (nonFailedTotal) / elapsedSeconds : 0.0;
        double abortRate = total > 0 ? (abortedCount * 100.0) / total : 0.0;
        double failRate = total > 0 ? (failedCount * 100.0) / total : 0.0;

        System.out.printf("Benchmark complete: %d total (%d completed, %d failed, %d aborted) in %.3fs%n",
                total, done, failedCount, abortedCount, elapsedSeconds);
        System.out.printf("Throughput: %.2f requests/second%n", throughput);
        System.out.printf("Abort rate: %.2f%%, Fail rate: %.2f%%%n", abortRate, failRate);
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

                String tx = sender + "," + receiver + "," + String.format(Locale.US, "%.2f", amount);
                txs.add(tx);
            }
        }

        // Shuffle for random order across shards
        Collections.shuffle(txs, rnd);
        writeTransactionsToFile(txs, "intra_shard_transfers_" + System.currentTimeMillis() + ".txt");
        logger.info("Built {} intra-shard transfer transactions.", txs.size());
        return txs;
    }

    /**
     * Build intra-shard transfers with 100% contention using a circular chain pattern.
     * Each transaction's receiver is the next transaction's sender:
     * 1→2, 2→3, 3→4, ..., N→1 (loops back to start)
     * This ensures every account is involved in exactly two consecutive transactions.
     *
     * @param accountToClusterIndex mapping of account IDs to their cluster index
     * @param totalRequests         number of transactions to generate (must be divisible by clusterCount)
     * @return list of CSV-formatted transactions "sender,receiver,amount"
     */
    public static List<String> buildLinearIntraShardTransfers(Map<Integer, Integer> accountToClusterIndex,
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

        // Sort accounts within each cluster for consistent ordering
        for (List<Integer> accounts : accountsByCluster.values()) {
            Collections.sort(accounts);
        }

        int perCluster = totalRequests / clusterCount;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        List<String> txs = new ArrayList<>(totalRequests);

        for (int clusterIdx = 0; clusterIdx < clusterCount; clusterIdx++) {
            List<Integer> accounts = accountsByCluster.get(clusterIdx);
            if (accounts == null || accounts.size() < 2) {
                throw new IllegalStateException("Not enough accounts in cluster " + clusterIdx);
            }

            int numAccounts = accounts.size();

            // Build circular chain: accounts[i % n] → accounts[(i+1) % n]
            for (int i = 0; i < perCluster; i++) {
                int sender = accounts.get(i % numAccounts);
                int receiver = accounts.get((i + 1) % numAccounts);
                double amount = rnd.nextDouble(1.0, 10.0);

                String tx = sender + "," + receiver + "," + String.format(Locale.US, "%.2f", amount);
                txs.add(tx);
            }
        }

        writeTransactionsToFile(txs, "half_contention_transfers_" + System.currentTimeMillis() + ".txt");
        logger.info("Built {} half-contention intra-shard transfer transactions.", txs.size());
        return txs;
    }

    /**
     * Build intra-shard transfers with configurable skew (contention level).
     * Skew controls how many accounts are "hot" - higher skew means fewer accounts
     * handle more transactions, increasing contention.
     *
     * @param accountToClusterIndex mapping of account IDs to their cluster index
     * @param totalRequests         number of transactions to generate (must be divisible by clusterCount)
     * @param skew                  value between 0.0 and 1.0:
     *                              0.0 = uniform distribution (no contention, all accounts used)
     *                              1.0 = maximum skew (only 2 accounts used per cluster)
     * @return list of CSV-formatted transactions "sender,receiver,amount"
     */
    public static List<String> buildIntraShardTransfersWithSkew(Map<Integer, Integer> accountToClusterIndex,
                                                                int totalRequests,
                                                                double skew) {
        if (skew < 0.0 || skew > 1.0) {
            throw new IllegalArgumentException("skew must be between 0.0 and 1.0");
        }

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

        // Sort accounts within each cluster for consistent ordering
        for (List<Integer> accounts : accountsByCluster.values()) {
            Collections.sort(accounts);
        }

        int perCluster = totalRequests / clusterCount;
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        List<String> txs = new ArrayList<>(totalRequests);

        for (int clusterIdx = 0; clusterIdx < clusterCount; clusterIdx++) {
            List<Integer> accounts = accountsByCluster.get(clusterIdx);
            if (accounts == null || accounts.size() < 2) {
                throw new IllegalStateException("Not enough accounts in cluster " + clusterIdx);
            }

            int numAccounts = accounts.size();
            // Calculate hot account pool size: ranges from numAccounts (skew=0) to 2 (skew=1)
            int hotPoolSize = Math.max(2, (int) Math.round(numAccounts * (1.0 - skew)));

            for (int i = 0; i < perCluster; i++) {
                int senderIdx = rnd.nextInt(hotPoolSize);
                int receiverIdx;
                do {
                    receiverIdx = rnd.nextInt(hotPoolSize);
                } while (receiverIdx == senderIdx);

                int sender = accounts.get(senderIdx);
                int receiver = accounts.get(receiverIdx);
                double amount = rnd.nextDouble(1.0, 10.0);

                String tx = sender + "," + receiver + "," + String.format(Locale.US, "%.2f", amount);
                txs.add(tx);
            }
        }

        Collections.shuffle(txs, rnd);
//        writeTransactionsToFile(txs, "skewed_transfers_" + System.currentTimeMillis() + ".txt");
        logger.info("Built {} intra-shard transfers with skew={} (hot pool size per cluster: {})",
                txs.size(), skew, Math.max(2, (int) Math.round(accountsByCluster.values().iterator().next().size() * (1.0 - skew))));
        return txs;
    }

    /**
     * Persist a set of transactions to a newline-delimited text file.
     *
     * @param transactions list of CSV-formatted transactions
     * @param outputPath   target path (relative or absolute). Parent directories are created if needed.
     */
    public static void writeTransactionsToFile(List<String> transactions, String outputPath) {
        Objects.requireNonNull(transactions, "transactions");
        Objects.requireNonNull(outputPath, "outputPath");

        Path path = Paths.get(outputPath);
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(path, transactions, StandardCharsets.UTF_8);
            logger.info("Wrote {} transactions to {}", transactions.size(), path.toAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to write transactions to " + path + ": " + e.getMessage(), e);
        }
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

    public long getCompletedCount() { return completed.sum(); }
    public long getAbortedCount() { return aborted.sum(); }
    public long getFailedCount() { return failed.sum(); }
    public long getTotalCount() { return completed.sum() + aborted.sum() + failed.sum(); }

    public double getAbortRate() {
        long total = getTotalCount();
        return total > 0 ? (aborted.sum() * 100.0) / total : 0.0;
    }

    /**
     * Get elapsed seconds for non-failed requests (from start to last non-failed completion).
     */
    public double getElapsedSeconds() {
        long nonFailed = getCompletedCount() + getAbortedCount();
        if (nonFailed > 0) {
            long lastCompletion = lastNonFailedCompletionNanos.get();
            if (lastCompletion > 0) {
                return (lastCompletion - startTimeNanos) / 1_000_000_000.0;
            }
        }
        // Fallback
        long effectiveEnd = (endTimeNanos == 0L) ? System.nanoTime() : endTimeNanos;
        return (effectiveEnd - startTimeNanos) / 1_000_000_000.0;
    }

    public double getThroughput() {
        double elapsed = getElapsedSeconds();
        long nonFailed = getCompletedCount() + getAbortedCount();
        return elapsed > 0 ? nonFailed / elapsed : 0.0;
    }

    public double getAvgLatencyMs() {
        long nonFailed = getCompletedCount() + getAbortedCount();
        return nonFailed > 0 ? totalLatencyNonFailedMillis.get() / (double) nonFailed : 0.0;
    }

    public long getMinLatencyMs() {
        long min = minLatencyNonFailedMillis.get();
        return min == Long.MAX_VALUE ? 0 : min;
    }

    public long getMaxLatencyMs() {
        return maxLatencyNonFailedMillis.get();
    }
}
