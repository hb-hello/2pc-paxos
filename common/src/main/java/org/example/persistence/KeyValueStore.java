package org.example.persistence;

import java.util.List;
import java.util.Map;

public interface KeyValueStore<T> {
    void put(int id, T entry);
    T get(int key);
    void delete(int key);
    void putAll(Map<Integer, T> entries);
    void close();
}
