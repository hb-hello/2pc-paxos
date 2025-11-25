package org.example.persistence;

public class DatabaseManager {
    public static KeyValueStore<Double> create(int nodeId) {
        return new ChronicleKeyValueStore<>(nodeId, Double.class);
    }
}
