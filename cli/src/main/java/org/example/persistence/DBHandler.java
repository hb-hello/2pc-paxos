package org.example.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.config.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class DBHandler {
    private static final Logger logger = LogManager.getLogger(DBHandler.class);

    private final Map<Integer, Map<Integer, KeyValueStore<Double>>> databases;
    // Mapping from accountId -> clusterIndex
    private final Map<Integer, Integer> accountIdToClusterIndex;

    // Shared executor used for all node- and account-level tasks. Sized to number of servers.
    private final ExecutorService executor;

    public DBHandler() {
        this.databases = createDBs();
        this.accountIdToClusterIndex = new HashMap<>();

        this.executor = Executors.newFixedThreadPool(Math.max(1, Config.getServerCount()));
        initialize();
    }

    private void clearDataDirectory() {
        Path dataDir = Paths.get("data");
        if (!Files.exists(dataDir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dataDir)) {
            walk.sorted(Comparator.reverseOrder())
                .filter(path -> !path.equals(dataDir))
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to delete " + path, e);
                    }
                });
        } catch (IOException e) {
            throw new RuntimeException("Failed to clear data directory", e);
        }
    }

    private Map<Integer, Map<Integer, KeyValueStore<Double>>> createDBs() {
        clearDataDirectory();
        Map<Integer, Map<Integer, KeyValueStore<Double>>> dbs = new HashMap<>();
        int clusterCount = Config.getServerClusterCount();
        int clusterSize = Config.getServerClusterSize();
        try {
            for (int clusterIndex = 0; clusterIndex < clusterCount; clusterIndex++) {
                Map<Integer, KeyValueStore<Double>> clusterMap = dbs.computeIfAbsent(clusterIndex, k -> new HashMap<>());
                for (int nodeIndex = 1; nodeIndex <= clusterSize; nodeIndex++) {
                    int nodeId = clusterIndex * clusterSize + nodeIndex;
                    KeyValueStore<Double> store = DatabaseManager.create(nodeId);
                    // Put the nodeId -> store mapping into the cluster map
                    clusterMap.put(nodeId, store);
                }
            }
            logger.info("Created databases for {} clusters with {} nodes each.", clusterCount, clusterSize);
            return dbs;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void initialize() {
        int clusterCount = Config.getServerClusterCount();
        int databaseSize = Config.getDatabaseSize();
        int perClusterDBSize = databaseSize / clusterCount;

        if (databaseSize % clusterCount != 0) {
            throw new RuntimeException("Database size must be divisible by cluster count.");
        }

        try {
            logger.info("Initializing databases with {} accounts across {} clusters ({} accounts per cluster)",
                    databaseSize, clusterCount, perClusterDBSize);
            System.out.println("Initializing databases with " + databaseSize + " accounts across " +
                    clusterCount + " clusters (" + perClusterDBSize + " accounts per cluster)");
            for (int accountId = 1; accountId <= databaseSize; accountId++) {
                int clusterIndex = (accountId - 1) / perClusterDBSize;

                // record this accountId as belonging to the computed clusterIndex
                accountIdToClusterIndex.put(accountId, clusterIndex);

                // insert the account into every node's KeyValueStore in this cluster
                for (int i = 0; i < clusterCount; i++) {
                    Map<Integer, KeyValueStore<Double>> clusterDbs = databases.get(i);
                    if (clusterDbs == null || clusterDbs.isEmpty()) {
                        throw new RuntimeException("No databases configured for cluster " + clusterIndex);
                    }
                    for (KeyValueStore<Double> database : clusterDbs.values()) {
                        if (i == clusterIndex) database.put(accountId, 10.0);
                        database.putClusterId(accountId, clusterIndex);
                    }
                }
            }
            logger.info("Database initialization complete.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Getter to retrieve the accountId -> clusterIndex mapping
    public Map<Integer, Integer> getAccountIdToClusterIndex() {
        return accountIdToClusterIndex;
    }

    public int getClusterIndexForAccount(int accountId) {
        Integer clusterIndex = accountIdToClusterIndex.get(accountId);
        if (clusterIndex == null) {
            throw new RuntimeException("Account ID " + accountId + " not found in any cluster.");
        }
        return clusterIndex;
    }

    public String getAccountEntry(int id) {
        Integer clusterIndex = accountIdToClusterIndex.get(id);
        if (clusterIndex == null) {
            throw new RuntimeException("Account ID " + id + " not found in any cluster.");
        }

        Map<Integer, KeyValueStore<Double>> clusterDbs = databases.get(clusterIndex);
        if (clusterDbs == null || clusterDbs.isEmpty()) {
            throw new RuntimeException("No databases configured for cluster " + clusterIndex);
        }

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, KeyValueStore<Double>> e : clusterDbs.entrySet()) {
            final Integer nodeId = e.getKey();
            final KeyValueStore<Double> db = e.getValue();
            try {
                Double accountEntry = db.get(id);
                sb.append(String.format("Node %d: Account %d -> %.2f%n", nodeId, id, accountEntry));
            } catch (Exception ex) {
                sb.append(String.format("Node %d: Account %d -> ERROR: %s%n", nodeId, id, ex.getMessage()));
            }
        }
        return sb.toString();
    }

    /**
     * Create a list of Runnables that reset the given account on each node in its cluster.
     * This only creates task objects; submission is the caller's responsibility so callers
     * can submit many tasks centrally without nesting executors.
     */
    private List<Runnable> createResetTasksForAccount(int accountId) {
        Integer clusterIndex = accountIdToClusterIndex.get(accountId);
        if (clusterIndex == null) {
            throw new RuntimeException("Account ID " + accountId + " not found in any cluster.");
        }

        Map<Integer, KeyValueStore<Double>> clusterDbs = databases.get(clusterIndex);
        if (clusterDbs == null || clusterDbs.isEmpty()) {
            throw new RuntimeException("No databases configured for cluster " + clusterIndex);
        }

        List<Runnable> tasks = new ArrayList<>();
        for (Map.Entry<Integer, KeyValueStore<Double>> e : clusterDbs.entrySet()) {
            final Integer nodeId = e.getKey();
            final KeyValueStore<Double> db = e.getValue();
            tasks.add(() -> {
                try {
                    db.put(accountId, 10.0);
                } catch (Exception ex) {
                    logger.warn("Failed to reset account {} on node {}: {}", accountId, nodeId, ex.getMessage());
                }
            });
        }
        return tasks;
    }

    private void updateClusterMetadataForAccount(int accountId, int clusterIndex) {
        for (Map<Integer, KeyValueStore<Double>> clusterDbs : databases.values()) {
            if (clusterDbs == null || clusterDbs.isEmpty()) {
                continue;
            }
            for (KeyValueStore<Double> db : clusterDbs.values()) {
                try {
                    db.putClusterId(accountId, clusterIndex);
                } catch (Exception ex) {
                    logger.warn("Failed to update cluster metadata for account {}: {}", accountId, ex.getMessage());
                }
            }
        }
    }

    /**
     * Reset a single account by submitting its per-node reset tasks to the shared executor
     * and waiting for them to complete.
     */
    public void resetAccount(int accountId) {
        List<Runnable> tasks = createResetTasksForAccount(accountId);
        List<Future<?>> futures = new ArrayList<>();
        for (Runnable r : tasks) {
            futures.add(executor.submit(r));
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception ex) {
                logger.warn("Error while waiting for reset task: {}", ex.getMessage());
            }
        }
    }

    /**
     * Reset all databases by submitting every per-node reset task for every account to the
     * shared executor. This avoids creating per-account executors and ensures the total
     * concurrency is capped by Config.getServerCount().
     */
    public void resetDatabases() {
        List<Integer> accountIds = new ArrayList<>(accountIdToClusterIndex.keySet());
        if (accountIds.isEmpty()) {
            return;
        }

        List<Future<?>> futures = new ArrayList<>();
        // Submit every per-node reset task for every account to the shared executor
        for (Integer accountId : accountIds) {
            List<Runnable> tasks = createResetTasksForAccount(accountId);
            for (Runnable r : tasks) {
                futures.add(executor.submit(r));
            }
        }

        // Wait for all tasks to finish
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception ex) {
                logger.warn("Exception while waiting for reset task: {}", ex.getMessage());
            }
        }
    }

    public void moveAccount(int accountId, int targetClusterIndex) {
        Integer currentClusterIndex = accountIdToClusterIndex.get(accountId);
        if (currentClusterIndex == null) {
            throw new RuntimeException("Account ID " + accountId + " not found in any cluster.");
        }
        if (currentClusterIndex == targetClusterIndex) {
            return; // No movement needed
        }

        try {
            // Find a source Account from one of the current cluster databases before deleting
            Double sourceAccountEntry = null;
            Map<Integer, KeyValueStore<Double>> currentClusterDbs = databases.get(currentClusterIndex);
            if (currentClusterDbs != null) {
                for (KeyValueStore<Double> database : currentClusterDbs.values()) {
                    try {
                        // Attempt to read from the replica if we haven't yet found the source account
                        if (sourceAccountEntry == null) {
                            Double a = database.get(accountId);
                            if (a != null) {
                                sourceAccountEntry = a;
                            }
                        }
                    } catch (Exception ignored) {
                        // continue; we still attempt to delete below for best-effort
                    }

                    try {
                        database.delete(accountId);
                    } catch (Exception ignored) {
                        // ignore per-replica delete failures
                    }
                }
            }

            if (sourceAccountEntry == null) {
                logger.warn("Account ID {} not found in current cluster {} during move operation, defaulting to initial balance.", accountId, currentClusterIndex);
                sourceAccountEntry = 10.0;
            }

            // Add to target cluster (insert the same fetched Account)
            Map<Integer, KeyValueStore<Double>> targetClusterDbs = databases.get(targetClusterIndex);
            if (targetClusterDbs != null) {
                for (KeyValueStore<Double> database : targetClusterDbs.values()) {
                    database.put(accountId, sourceAccountEntry);
                }
            } else {
                throw new RuntimeException("Target cluster " + targetClusterIndex + " not configured.");
            }

            updateClusterMetadataForAccount(accountId, targetClusterIndex);

            // Update mapping
            accountIdToClusterIndex.put(accountId, targetClusterIndex);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        for (Map<Integer, KeyValueStore<Double>> clusterDbs : databases.values()) {
            for (KeyValueStore<Double> db : clusterDbs.values()) {
                try {
                    db.close();
                } catch (Exception e) {
                    logger.warn("Exception while closing database: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * Shutdown the shared executor (call during application shutdown).
     */
    public void shutdown() {
        try {
            close();
            executor.shutdownNow();
        } catch (Exception e) {
            logger.warn("Exception while shutting down DBHandler executor: {}", e.getMessage());
        }
    }

}
