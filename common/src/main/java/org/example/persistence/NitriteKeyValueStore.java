package org.example.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.collection.NitriteCollection;
import org.dizitart.no2.exceptions.UniqueConstraintException;
import org.dizitart.no2.exceptions.ValidationException;
import org.dizitart.no2.mvstore.MVStoreModule;
import org.dizitart.no2.repository.ObjectRepository;
import org.dizitart.no2.index.IndexType;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NitriteKeyValueStore<T> implements KeyValueStore<T> {
    private static final Logger logger = LogManager.getLogger(NitriteKeyValueStore.class);

    private final int nodeId;
    private final String indexFieldName;
    private final Class<T> entityClass;
    private Nitrite db;
    private ObjectRepository<T> repository;
    private final Map<Integer, Integer> clusterIdMap = new ConcurrentHashMap<>();

    public NitriteKeyValueStore(int nodeId, String indexFieldName, Class<T> entityClass) {
        this.nodeId = nodeId;
        this.indexFieldName = indexFieldName;
        this.entityClass = entityClass;
        try {
            build();
        } catch (Exception e) {
            logger.error("Failed to initialize NitriteKeyValueStore for node {}: {}", nodeId, e.getMessage());
        }
    }

    private void build() {
        // Initialize Nitrite database
        String dbPath = "data/server-" + nodeId + ".db";

        MVStoreModule storeModule = MVStoreModule.withConfig()
                .filePath(dbPath)
                .autoCommit(false)
                .build();

        this.db = Nitrite.builder()
                .loadModule(storeModule)
                .openOrCreate();

        // Create an object repository for the provided entity class
        this.repository = db.getRepository(entityClass);

        // Also ensure the underlying collection has a UNIQUE index on the configured field.
        try {
            String collectionName = entityClass.getSimpleName();
            NitriteCollection collection = db.getCollection(collectionName);
            collection.createIndex(indexFieldName, IndexType.UNIQUE);
        } catch (Exception e) {
            // If index creation fails, log and continue — repository may still be usable depending on schema
            logger.warn("Failed to create unique index '{}' on repository {}: {}", indexFieldName, entityClass.getName(), e.getMessage());
        }
    }

    @Override
    public void put(int id, T entry) {
        try {
            repository.update(entry, true);
        } catch (ValidationException e) {
            logger.error("Validation error (null check) when inserting entry into repository {}: {}", entityClass.getName(), e.getMessage());
        } catch (UniqueConstraintException e) {
            logger.error("Unique constraint violation when inserting entry into repository {}: {}", entityClass.getName(), e.getMessage());
        }
    }

    @Override
    public T get(int key) {
        try {
            return repository.getById(key);
        } catch (Exception e) {
            logger.error("Error retrieving entry with key {} from repository {}: {}", key, entityClass.getName(), e.getMessage());
            return null;
        }
    }

    @Override
    public void delete(int key) {
        try {
            repository.remove(org.dizitart.no2.filters.FluentFilter.where(indexFieldName).eq(key));
        } catch (Exception e) {
            logger.error("Error deleting entry with key {} from repository {}: {}", key, entityClass.getName(), e.getMessage());
        }
    }

    @Override
    public void putAll(Map<Integer, T> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }
        for (Map.Entry<Integer, T> en : entries.entrySet()) {
            T entry = en.getValue();
            try {
                repository.update(entry, true);
            } catch (Exception ex) {
                logger.warn("Failed to put entry with id {} into repository {}: {}", en.getKey(), entityClass.getName(), ex.getMessage());
            }
        }
    }

    @Override
    public void close() {
        try {
            if (db != null && !db.isClosed()) {
                db.commit();
                db.close();
            }
        } catch (Exception e) {
            logger.warn("Failed to commit/close Nitrite DB for node {}: {}", nodeId, e.getMessage());
        } finally {
            repository = null;
        }
    }

    @Override
    public void putClusterId(int key, int clusterId) {
        clusterIdMap.put(key, clusterId);
    }

    @Override
    public Integer getClusterId(int key) {
        return clusterIdMap.get(key);
    }

    @Override
    public void deleteClusterId(int key) {
        clusterIdMap.remove(key);
    }
}
