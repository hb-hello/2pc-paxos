package org.example.persistence;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ChronicleKeyValueStore<T> implements KeyValueStore<T> {
    private static final Logger logger = LogManager.getLogger(ChronicleKeyValueStore.class);

    private final int nodeId;
    private final Class<T> valueClass;
    private ChronicleMap<Integer, T> map;

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
        File file = new File("data/chronicle-node-" + nodeId + ".dat");

        ChronicleMapBuilder<Integer, T> builder = ChronicleMapBuilder
                .of(Integer.class, valueClass)
                .name("kv-store-node-" + nodeId)
                .entries(10_000)
                .constantKeySizeBySample(0)
                .constantValueSizeBySample(createSampleValue());

        if (!file.exists()) {
            file.getParentFile().mkdirs();
            file.createNewFile();
        }

        this.map = builder.createPersistedTo(file);
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
}
