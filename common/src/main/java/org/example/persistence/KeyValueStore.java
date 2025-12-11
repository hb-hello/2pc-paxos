package org.example.persistence;

import java.util.Map;

public interface KeyValueStore<T> {
    void put(int id, T entry);
    T get(int key);
    void delete(int key);
    void putAll(Map<Integer, T> entries);
    void close();

    void putClusterId(int key, int clusterId);
    Integer getClusterId(int key);
    void deleteClusterId(int key);
    Map<Integer, Integer> getAllClusterIds();

    void putWalEntry(String compositeKey, Double beforeBalance);
    Double getWalEntry(String compositeKey);
    void deleteWalEntry(String compositeKey);
}
