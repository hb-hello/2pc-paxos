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
     * Build a mixed workload with configurable read/write ratio, cross-shard ratio, and skew.
     *
     * @param accountToClusterIndex mapping of account IDs to their cluster index
     * @param totalRequests         total number of requests to generate
     * @param skew                  value between 0.0 and 1.0 controlling contention for write transactions:
     *                              0.0 = uniform distribution (all accounts used)
     *                              1.0 = maximum skew (only 2 accounts used per cluster)
     * @param readWriteRatio        value between 0.0 and 1.0:
     *                              0.0 = all requests are balance reads
     *                              1.0 = all requests are transfers (writes)
     * @param crossShardRatio       value between 0.0 and 1.0 (applies to transfers only):
     *                              0.0 = all transfers are intra-shard
     *                              1.0 = all transfers are cross-shard
     * @return list of CSV-formatted requests:
     *         - balance reads: "accountId" (single value)
     *         - transfers: "sender,receiver,amount" (three values)
     */
    public static List<String> buildMixedWorkload(Map<Integer, Integer> accountToClusterIndex,
                                                  int totalRequests,
                                                  double skew,
                                                  double readWriteRatio,
                                                  double crossShardRatio) {
        validateRatio(skew, "skew");
        validateRatio(readWriteRatio, "readWriteRatio");
        validateRatio(crossShardRatio, "crossShardRatio");

        int clusterCount = Config.getServerClusterCount();

        // Group accounts by cluster
        Map<Integer, List<Integer>> accountsByCluster = groupAccountsByCluster(accountToClusterIndex);

        // Sort accounts within each cluster for consistent ordering
        for (List<Integer> accounts : accountsByCluster.values()) {
            Collections.sort(accounts);
        }

        // Pre-compute hot pools for consistent skew across all transfer types
        // Use Math.max(2, ...) to ensure we have at least 2 accounts for intra-shard transfers
        Map<Integer, List<Integer>> hotPoolByCluster = computeHotPools(accountsByCluster, skew, clusterCount);

        // Calculate request counts
        int writeRequests = (int) Math.round(totalRequests * readWriteRatio);
        int readRequests = totalRequests - writeRequests;
        int crossShardTransfers = (int) Math.round(writeRequests * crossShardRatio);
        int intraShardTransfers = writeRequests - crossShardTransfers;

        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        List<String> requests = new ArrayList<>(totalRequests);

        // Build balance read requests (uniform distribution across all accounts)
        List<Integer> allAccounts = new ArrayList<>(accountToClusterIndex.keySet());
        for (int i = 0; i < readRequests; i++) {
            int accountId = allAccounts.get(rnd.nextInt(allAccounts.size()));
            requests.add(String.valueOf(accountId));
        }

        // Build intra-shard transfer requests using shared hot pools
        requests.addAll(buildIntraShardTransfersInternal(hotPoolByCluster, intraShardTransfers, rnd));

        // Build cross-shard transfer requests using shared hot pools
        requests.addAll(buildCrossShardTransfersInternal(hotPoolByCluster, crossShardTransfers, clusterCount, rnd));

        // Shuffle all requests for random order
        Collections.shuffle(requests, rnd);

        logger.info("Built mixed workload: {} total ({} reads, {} intra-shard transfers, {} cross-shard transfers), skew={}",
                requests.size(), readRequests, intraShardTransfers, crossShardTransfers, skew);
        writeTransactionsToFile(requests, "benchmarks/inputs/generated_benchmark_input" + System.currentTimeMillis() + ".txt");
        return requests;
    }

    /**
     * Compute hot pools for each cluster based on skew.
     * Hot pool contains the subset of accounts that will receive contention.
     */
    private static Map<Integer, List<Integer>> computeHotPools(Map<Integer, List<Integer>> accountsByCluster,
                                                                double skew,
                                                                int clusterCount) {
        Map<Integer, List<Integer>> hotPoolByCluster = new HashMap<>();
        for (int clusterIdx = 0; clusterIdx < clusterCount; clusterIdx++) {
            List<Integer> accounts = accountsByCluster.get(clusterIdx);
            if (accounts != null && accounts.size() >= 2) {
                int numAccounts = accounts.size();
                // Hot pool size ranges from numAccounts (skew=0) to 2 (skew=1)
                // Need at least 2 for intra-shard transfers (sender != receiver)
                int hotPoolSize = Math.max(2, (int) Math.round(numAccounts * (1.0 - skew)));
                hotPoolByCluster.put(clusterIdx, new ArrayList<>(accounts.subList(0, hotPoolSize)));
            }
        }
        return hotPoolByCluster;
    }

    /**
     * Validate that a ratio parameter is between 0.0 and 1.0.
     */
    private static void validateRatio(double ratio, String paramName) {
        if (ratio < 0.0 || ratio > 1.0) {
            throw new IllegalArgumentException(paramName + " must be between 0.0 and 1.0, got: " + ratio);
        }
    }

    /**
     * Group account IDs by their cluster index.
     */
    private static Map<Integer, List<Integer>> groupAccountsByCluster(Map<Integer, Integer> accountToClusterIndex) {
        Map<Integer, List<Integer>> accountsByCluster = new HashMap<>();
        for (Map.Entry<Integer, Integer> e : accountToClusterIndex.entrySet()) {
            int accountId = e.getKey();
            int clusterIdx = e.getValue();
            accountsByCluster
                    .computeIfAbsent(clusterIdx, k -> new ArrayList<>())
                    .add(accountId);
        }
        return accountsByCluster;
    }

    /**
     * Build intra-shard transfers using pre-computed hot pools (internal helper, does not shuffle).
     */
    private static List<String> buildIntraShardTransfersInternal(Map<Integer, List<Integer>> hotPoolByCluster,
                                                                  int totalTransfers,
                                                                  ThreadLocalRandom rnd) {
        if (totalTransfers == 0) {
            return new ArrayList<>();
        }

        List<String> txs = new ArrayList<>(totalTransfers);

        // Get clusters that have valid hot pools (at least 2 accounts for intra-shard)
        List<Integer> validClusters = new ArrayList<>();
        for (Map.Entry<Integer, List<Integer>> entry : hotPoolByCluster.entrySet()) {
            if (entry.getValue().size() >= 2) {
                validClusters.add(entry.getKey());
            }
        }

        if (validClusters.isEmpty()) {
            logger.warn("No clusters with enough accounts for intra-shard transfers");
            return new ArrayList<>();
        }

        // Distribute transfers evenly across valid clusters
        int basePerCluster = totalTransfers / validClusters.size();
        int remainder = totalTransfers % validClusters.size();

        int clusterIdx = 0;
        for (int cluster : validClusters) {
            List<Integer> hotPool = hotPoolByCluster.get(cluster);
            int hotPoolSize = hotPool.size();

            // This cluster gets basePerCluster + 1 if it's one of the first 'remainder' clusters
            int transfersForThisCluster = basePerCluster + (clusterIdx < remainder ? 1 : 0);

            for (int i = 0; i < transfersForThisCluster; i++) {
                int senderIdx = rnd.nextInt(hotPoolSize);
                int receiverIdx;
                do {
                    receiverIdx = rnd.nextInt(hotPoolSize);
                } while (receiverIdx == senderIdx);

                int sender = hotPool.get(senderIdx);
                int receiver = hotPool.get(receiverIdx);
                double amount = rnd.nextDouble(1.0, 10.0);

                String tx = sender + "," + receiver + "," + String.format(Locale.US, "%.2f", amount);
                txs.add(tx);
            }
            clusterIdx++;
        }

        return txs;
    }

    /**
     * Build cross-shard transfers using pre-computed hot pools (internal helper, does not shuffle).
     * Cross-shard transfers have sender and receiver in different clusters.
     */
    private static List<String> buildCrossShardTransfersInternal(Map<Integer, List<Integer>> hotPoolByCluster,
                                                                  int totalTransfers,
                                                                  int clusterCount,
                                                                  ThreadLocalRandom rnd) {
        if (totalTransfers == 0 || clusterCount < 2) {
            return new ArrayList<>();
        }

        List<Integer> clusterIndices = new ArrayList<>(hotPoolByCluster.keySet());
        if (clusterIndices.size() < 2) {
            logger.warn("Not enough clusters with accounts for cross-shard transfers");
            return new ArrayList<>();
        }

        List<String> txs = new ArrayList<>(totalTransfers);

        for (int i = 0; i < totalTransfers; i++) {
            // Pick two different clusters
            int senderClusterIdx = clusterIndices.get(rnd.nextInt(clusterIndices.size()));
            int receiverClusterIdx;
            do {
                receiverClusterIdx = clusterIndices.get(rnd.nextInt(clusterIndices.size()));
            } while (receiverClusterIdx == senderClusterIdx);

            List<Integer> senderHotPool = hotPoolByCluster.get(senderClusterIdx);
            List<Integer> receiverHotPool = hotPoolByCluster.get(receiverClusterIdx);

            int sender = senderHotPool.get(rnd.nextInt(senderHotPool.size()));
            int receiver = receiverHotPool.get(rnd.nextInt(receiverHotPool.size()));
            double amount = rnd.nextDouble(1.0, 10.0);

            String tx = sender + "," + receiver + "," + String.format(Locale.US, "%.2f", amount);
            txs.add(tx);
        }

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
