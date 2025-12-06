package org.example.state;

import org.example.CheckpointMessage;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe manager for Paxos checkpoints.
 * Stores all checkpoints (sequence number + snapshot) and provides
 * accessors for the most recent checkpointed state as well as historical lookups.
 */
public class CheckpointManager {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // All checkpoints keyed by sequence number (sorted)
    private final NavigableMap<Long, String> checkpoints = new TreeMap<>();

    /**
     * Record a new checkpoint. Duplicate sequence numbers are overwritten.
     *
     * @param seqNum   the sequence number up to which state is checkpointed
     * @param snapshot serialized state snapshot (e.g. JSON or base64)
     * @return true if this is a new (or updated) checkpoint
     */
    public boolean addCheckpoint(long seqNum, String snapshot) {
        lock.writeLock().lock();
        try {
            String prev = checkpoints.put(seqNum, snapshot);
            return prev == null || !prev.equals(snapshot);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @return the sequence number of the latest checkpoint, or -1 if none exists
     */
    public long getLatestCheckpointedSeqNum() {
        lock.readLock().lock();
        try {
            return checkpoints.isEmpty() ? 0L : checkpoints.lastKey();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @return the latest checkpoint as a protobuf message, or null if no checkpoint exists
     */
    public CheckpointMessage getLatestCheckpointMessage() {
        lock.readLock().lock();
        try {
            if (checkpoints.isEmpty()) {
                return null;
            }
            long seqNum = checkpoints.lastKey();
            String snapshot = checkpoints.get(seqNum);
            return CheckpointMessage.newBuilder()
                    .setSequenceNumber(seqNum)
                    .setState(snapshot)
                    .build();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @return the raw snapshot string for the latest checkpoint, or null if none exists
     */
    public String getLatestSnapshot() {
        lock.readLock().lock();
        try {
            return checkpoints.isEmpty() ? null : checkpoints.lastEntry().getValue();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get a checkpoint message by sequence number.
     *
     * @param seqNum the sequence number to look up
     * @return the checkpoint message, or null if not found
     */
    public CheckpointMessage getCheckpointMessage(long seqNum) {
        lock.readLock().lock();
        try {
            String snapshot = checkpoints.get(seqNum);
            if (snapshot == null) {
                return null;
            }
            return CheckpointMessage.newBuilder()
                    .setSequenceNumber(seqNum)
                    .setState(snapshot)
                    .build();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the snapshot string by sequence number.
     *
     * @param seqNum the sequence number to look up
     * @return the snapshot string, or null if not found
     */
    public String getSnapshot(long seqNum) {
        lock.readLock().lock();
        try {
            return checkpoints.get(seqNum);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @return the number of stored checkpoints
     */
    public int size() {
        lock.readLock().lock();
        try {
            return checkpoints.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Reset the checkpoint state (useful for testing or server reset).
     */
    public void reset() {
        lock.writeLock().lock();
        try {
            checkpoints.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
