package org.example.persistence;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class ChronicleKeyValueStore<T> implements KeyValueStore<T> {
    private static final Logger logger = LogManager.getLogger(ChronicleKeyValueStore.class);

    private final int nodeId;
    private final Class<T> valueClass;
    private ChronicleMap<Integer, T> map;
    private ChronicleMap<Integer, Integer> clusterIdMap;
    private ChronicleMap<CharSequence, Double> walMap;

    public ChronicleKeyValueStore(int nodeId, Class<T> valueClass) {
        this.nodeId = nodeId;
        this.valueClass = valueClass;
        try {
            build();
        } catch (Exception e) {
            logger.error("Failed to initialize ChronicleKeyValueStore for node {}: {}", nodeId, e.getMessage());
        }
    }

    private void build() throws IOException {
        File file = new File("data/balances-node-" + nodeId + ".dat");
        File clusterFile = new File("data/cluster-map-node-" + nodeId + ".dat");
        File walFile = new File("data/wal-node-" + nodeId + ".dat");

        ChronicleMapBuilder<Integer, T> builder = ChronicleMapBuilder
                .of(Integer.class, valueClass)
                .name("balances-store-node-" + nodeId)
                .entries(10_000)
                .constantKeySizeBySample(0)
                .constantValueSizeBySample(createSampleValue());

        ChronicleMapBuilder<Integer, Integer> clusterBuilder = ChronicleMapBuilder
                .of(Integer.class, Integer.class)
                .name("cluster-map-node-" + nodeId)
                .entries(10_000)
                .constantKeySizeBySample(0)
                .constantValueSizeBySample(0);
        ChronicleMapBuilder<CharSequence, Double> walBuilder = ChronicleMapBuilder
                .of(CharSequence.class, Double.class)
                .name("wal-store-node-" + nodeId)
                .entries(10_000)
                .averageKeySize(32)
                .constantValueSizeBySample(10.0);

        if (!file.exists()) {
            file.getParentFile().mkdirs();
            file.createNewFile();
        }
        if (!clusterFile.exists()) {
            clusterFile.getParentFile().mkdirs();
            clusterFile.createNewFile();
        }
        if (!walFile.exists()) {
            walFile.getParentFile().mkdirs();
            walFile.createNewFile();
        }

        this.map = builder.createPersistedTo(file);
        this.clusterIdMap = clusterBuilder.createPersistedTo(clusterFile);
        this.walMap = walBuilder.createPersistedTo(walFile);
    }

    @Override
    public void put(int key, T entry) {
        try {
            map.put(key, entry);
        } catch (Exception e) {
            logger.error("Error putting entry into Chronicle map: {}", e.getMessage());
        }
    }

    @Override
    public T get(int key) {
        try {
            return map.get(key);
        } catch (Exception e) {
            logger.error("Error getting entry with key {} from Chronicle map: {}", key, e.getMessage());
            return null;
        }
    }

    @Override
    public void delete(int key) {
        try {
            map.remove(key);
        } catch (Exception e) {
            logger.error("Error deleting entry with key {} from Chronicle map: {}", key, e.getMessage());
        }
    }

    @Override
    public void putAll(Map<Integer, T> entries) {
        try {
            map.putAll(entries);
        } catch (Exception e) {
            logger.error("Error putting all entries into Chronicle map: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        try {
            if (map != null) {
                map.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing Chronicle map for node {}: {}", nodeId, e.getMessage());
        }
        try {
            if (clusterIdMap != null) {
                clusterIdMap.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing Chronicle cluster map for node {}: {}", nodeId, e.getMessage());
        }
        try {
            if (walMap != null) {
                walMap.close();
            }
        } catch (Exception e) {
            logger.warn("Error closing Chronicle WAL map for node {}: {}", nodeId, e.getMessage());
        }
    }

    public T createSampleValue() {
        try {
            // handle common Java wrapper types and String
            if (valueClass == Double.class) {
                return valueClass.cast(0.0d);
            }
            if (valueClass == Integer.class) {
                return valueClass.cast(0);
            }
            if (valueClass == Long.class) {
                return valueClass.cast(0L);
            }
            if (valueClass == Float.class) {
                return valueClass.cast(0.0f);
            }
            if (valueClass == Byte.class) {
                return valueClass.cast((byte) 0);
            }
            if (valueClass == Boolean.class) {
                return valueClass.cast(false);
            }
            if (valueClass == Character.class) {
                return valueClass.cast('\0');
            }
            if (valueClass == String.class) {
                return valueClass.cast("");
            }
            // enums: pick first constant if available
            if (valueClass.isEnum()) {
                Object[] consts = valueClass.getEnumConstants();
                if (consts != null && consts.length > 0) {
                    return valueClass.cast(consts[0]);
                }
            }
            // fallback to no-arg constructor
            return valueClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create sample value for ChronicleMap sizing", e);
        }
    }

    @Override
    public void putClusterId(int key, int clusterId) {
        try {
            clusterIdMap.put(key, clusterId);
        } catch (Exception e) {
            logger.error("Error putting cluster id entry into Chronicle map: {}", e.getMessage());
        }
    }

    @Override
    public Integer getClusterId(int key) {
        try {
            return clusterIdMap.get(key);
        } catch (Exception e) {
            logger.error("Error getting cluster id with key {} from Chronicle map: {}", key, e.getMessage());
            return null;
        }
    }

    @Override
    public void deleteClusterId(int key) {
        try {
            clusterIdMap.remove(key);
        } catch (Exception e) {
            logger.error("Error deleting cluster id entry with key {} from Chronicle map: {}", key, e.getMessage());
        }
    }

    @Override
    public Map<Integer, Integer> getAllClusterIds() {
        return clusterIdMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public void putWalEntry(String compositeKey, Double beforeBalance) {
        try {
            walMap.put(compositeKey, beforeBalance);
        } catch (Exception e) {
            logger.error("Error putting WAL entry {} into Chronicle map: {}", compositeKey, e.getMessage());
        }
    }

    @Override
    public Double getWalEntry(String compositeKey) {
        try {
            return walMap.get(compositeKey);
        } catch (Exception e) {
            logger.error("Error getting WAL entry {} from Chronicle map: {}", compositeKey, e.getMessage());
            return null;
        }
    }

    @Override
    public void deleteWalEntry(String compositeKey) {
        try {
            walMap.remove(compositeKey);
        } catch (Exception e) {
            logger.error("Error deleting WAL entry {} from Chronicle map: {}", compositeKey, e.getMessage());
        }
    }
}
