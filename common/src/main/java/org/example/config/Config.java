package org.example.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class Config {
    private static final Logger logger = LogManager.getLogger(Config.class);

    private static final String DEFAULT_CONFIG_PATH = "config.properties";
    private static String activeConfigPath = DEFAULT_CONFIG_PATH;

    private static boolean initialized = false;

    // Static configuration fields (loaded in initialize)
    private static String transactionsSetsPath;
    private static int serverClusterCount;
    private static int serverClusterSize;
    private static int serverPortStart;
    private static int clientPort;
    private static int clientTimeoutMillis;
    private static int serverTimeoutMillis;
    private static int checkpointInterval;
    private static int databaseSize;
    private static String serverExecutablePath;

    private static int quorumSize;
    private static final HashMap<Integer, Integer> serverIdToClusterIndexMap = new HashMap<>();
    private static final HashMap<Integer, List<Integer>> clusterIndexToServerIdMap = new HashMap<>();

    // Map of server id -> NodeDetails (in insertion order)
    private static final Map<Integer, NodeDetails> nodes = new LinkedHashMap<>();

    // Private constructor to prevent instantiation
    private Config() {
        throw new UnsupportedOperationException("Config is a utility class and cannot be instantiated");
    }

    private static void ensureInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Config has not been initialized. Call Config.initialize() first.");
        }
    }

    /**
     * Initialize configuration from the provided Properties object.
     * Generates server ids 1, 2, 3 ... and assigns sequential ports starting from server.port.start.
     * Also inserts a client node at id 0 with host 'localhost' and configured client port.
     */
    public static synchronized void initialize(Properties props) {
        if (props == null) {
            throw new IllegalArgumentException("props must not be null");
        }
        if (initialized) {
            logger.warn("Config.initialize called but Config is already initialized; call will be ignored.");
            return;
        }

        // Helper to parse integer properties with defaults
        java.util.function.BiFunction<String, Integer, Integer> parseIntOrDefault = (key, def) -> {
            String v = props.getProperty(key);
            if (v == null || v.isBlank()) return def;
            try {
                return Integer.parseInt(v.trim());
            } catch (NumberFormatException e) {
                logger.warn("Invalid integer for property {}, using default {}: {}", key, def, e.getMessage());
                return def;
            }
        };

        // Defaults matching the provided config.properties
        transactionsSetsPath = props.getProperty("transactions.sets.path", "src/main/resources/transactionSets.csv");
        serverClusterCount = parseIntOrDefault.apply("server.cluster.count", 3);
        serverClusterSize = parseIntOrDefault.apply("server.cluster.size", 3);
        serverPortStart = parseIntOrDefault.apply("server.port.start", 3000);
        clientPort = parseIntOrDefault.apply("client.port", 4000);
        clientTimeoutMillis = parseIntOrDefault.apply("client.timeout.millis", 2000);
        serverTimeoutMillis = parseIntOrDefault.apply("server.timeout.millis", 1000);
        checkpointInterval = parseIntOrDefault.apply("checkpoint.interval", 100);
        databaseSize = parseIntOrDefault.apply("database.size", 9000);
        serverExecutablePath = props.getProperty("server.executable.path", "server/target/server-1.0-SNAPSHOT-jar-with-dependencies.jar");

        // Build nodes map: first insert client at id 0, then server ids 1..N with ports sequential from serverPortStart
        nodes.clear();
        String clientHost = "localhost";
        nodes.put(0, new NodeDetails(0, clientHost, clientPort));

        int totalServers = Math.max(0, serverClusterCount * serverClusterSize);
        for (int i = 1; i <= totalServers; i++) {
            String host = "localhost";
            int port = serverPortStart + (i - 1);
            nodes.put(i, new NodeDetails(i, host, port));
            int clusterIndex = (i - 1) / serverClusterSize;
            serverIdToClusterIndexMap.put(i, clusterIndex);
            clusterIndexToServerIdMap.computeIfAbsent(clusterIndex, k -> new ArrayList<>()).add(i);
        }

        quorumSize = (serverClusterSize / 2) + 1;

        initialized = true;
        logger.info("Config initialized: {} servers configured (including client at 0), transactions path: {}, database.size={}, server.executable.path={}", nodes.size(), transactionsSetsPath, databaseSize, serverExecutablePath);
    }

    public static synchronized void reset() {
        initialized = false;
        nodes.clear();
        serverIdToClusterIndexMap.clear();
        clusterIndexToServerIdMap.clear();
    }

    /**
     * Convenience overload to load properties from a file path using existing loader.
     */
    public static void initialize(String filePath) {
        String resolvedPath = (filePath == null || filePath.isBlank()) ? DEFAULT_CONFIG_PATH : filePath;
        activeConfigPath = resolvedPath;
        Properties props = loadProperties(resolvedPath);
        initialize(props);
    }

    public static void initialize() {
        initialize(DEFAULT_CONFIG_PATH);
    }

    // Accessors (ensure initialized)
    public static String getTransactionsSetsPath() {
        ensureInitialized();
        return transactionsSetsPath;
    }

    public static int getServerClusterCount() {
        ensureInitialized();
        return serverClusterCount;
    }

    public static int getServerClusterSize() {
        ensureInitialized();
        return serverClusterSize;
    }

    public static int getServerPortStart() {
        ensureInitialized();
        return serverPortStart;
    }

    public static int getClientPort() {
        ensureInitialized();
        return clientPort;
    }

    public static int getClientTimeoutMillis() {
        ensureInitialized();
        return clientTimeoutMillis;
    }

    public static int getServerTimeoutMillis() {
        ensureInitialized();
        return serverTimeoutMillis;
    }

    public static int getCheckpointInterval() {
        ensureInitialized();
        return checkpointInterval;
    }

    public static int getDatabaseSize() {
        ensureInitialized();
        return databaseSize;
    }

    public static String getServerExecutablePath() {
        ensureInitialized();
        return serverExecutablePath;
    }

    public static Map<Integer, NodeDetails> getNodes() {
        ensureInitialized();
        return Collections.unmodifiableMap(nodes);
    }

    public static Map<Integer, NodeDetails> getNodesExcept(int excludeId) {
        ensureInitialized();
        Map<Integer, NodeDetails> filtered = new LinkedHashMap<>(nodes);
        filtered.remove(excludeId);
        return Collections.unmodifiableMap(filtered);
    }

    public static int getNodePort(int id) {
        ensureInitialized();
        NodeDetails node = nodes.get(id);
        if (node == null) {
            throw new IllegalArgumentException("No node found with id: " + id);
        }
        return node.port();
    }

    public static Map<Integer, NodeDetails> getServers() {
        ensureInitialized();
        Map<Integer, NodeDetails> servers = new LinkedHashMap<>(nodes);
        servers.remove(0); // Remove client node
        return Collections.unmodifiableMap(servers);
    }

    public static NodeDetails getServerDetails(int id) {
        ensureInitialized();
        return nodes.get(id);
    }

    public static int getServerCount() {
        ensureInitialized();
        return nodes.size() - 1;
    }

    public static Set<Integer> getAllServerIdsExcept(int excludeId) {
        ensureInitialized();
        Set<Integer> ids = new HashSet<>(nodes.keySet());
        ids.remove(0); // Remove client id
        ids.remove(excludeId);
        return ids;
    }

    public static Set<Integer> getAllServerIdsExceptInCluster(int clusterIndex) {
        ensureInitialized();
        Set<Integer> ids = new HashSet<>(nodes.keySet());
        ids.remove(0); // Remove client id
        for (Integer id : clusterIndexToServerIdMap.get(clusterIndex)) {
            ids.remove(id);
        }
        return ids;
    }

    public static List<Integer> getServerIdsInCluster(int clusterIndex) {
        ensureInitialized();
        List<Integer> ids = new ArrayList<>();
        for (Integer id : clusterIndexToServerIdMap.get(clusterIndex)) {
            if (id != 0) {
                ids.add(id);
            }
        }
        return ids;
    }

    public static List<Integer> getServerIdsInClusterExcept(int excludeId) {
        ensureInitialized();
        List<Integer> ids = new ArrayList<>();
        int clusterIndex = serverIdToClusterIndexMap.get(excludeId);
        for (Integer id : clusterIndexToServerIdMap.get(clusterIndex)) {
            if (id != 0 && id != excludeId) {
                ids.add(id);
            }
        }
        return ids;
    }

    public static int getServerClusterIndex(int serverId) {
        ensureInitialized();
        Integer index = serverIdToClusterIndexMap.get(serverId);
        if (index == null) {
            throw new IllegalArgumentException("No server found with id: " + serverId);
        }
        return index;
    }

    public static int getQuorumSize() {
        ensureInitialized();
        return quorumSize;
    }

    public static synchronized void updateClusterConfig(int clusterCount, int clusterSize) {
        Properties props = loadProperties(activeConfigPath);
        props.setProperty("server.cluster.count", Integer.toString(clusterCount));
        props.setProperty("server.cluster.size", Integer.toString(clusterSize));
        try (FileOutputStream out = new FileOutputStream(activeConfigPath)) {
            props.store(out, "Updated cluster configuration");
        } catch (IOException e) {
            throw new RuntimeException("Failed to write updated cluster config", e);
        }
        reset();
    }

    public static String getActiveConfigPath() {
        return activeConfigPath;
    }

    /**
     * Load properties from file
     */
    private static Properties loadProperties(String filePath) {
        Properties props = new Properties();

        // Try loading from file system first
        try (InputStream input = new FileInputStream(filePath)) {
            props.load(input);
            logger.info("Successfully loaded properties file from: {}", filePath);
        } catch (IOException e) {
            logger.warn("Could not load properties file from {}, trying classpath: {}", filePath, e.getMessage());

            // Try loading from classpath as fallback
            try (InputStream input = Config.class.getClassLoader().getResourceAsStream(filePath)) {
                if (input != null) {
                    props.load(input);
                    logger.info("Successfully loaded properties file from classpath: {}", filePath);
                } else {
                    logger.warn("Properties file not found in classpath, using default values");
                }
            } catch (IOException ex) {
                logger.warn("Could not load properties file from classpath, using default values: {}", ex.getMessage());
            }
        }

        return props;
    }
}