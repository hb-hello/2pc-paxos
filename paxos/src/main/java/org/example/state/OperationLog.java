package org.example.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.units.qual.A;
import org.example.ClientRequest;

import java.util.EnumMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class OperationLog {
    private static final Logger logger = LogManager.getLogger(OperationLog.class);

    private final EnumMap<OperationStatus, Integer> STATUS_ORDER;
    private final ConcurrentMap<Long, OperationLogEntry> entries;
    private final AtomicLong nextSeqNum;

    //map of OperationLogEntry keyed by seqNum

    public OperationLog() {
        this.STATUS_ORDER = new EnumMap<>(OperationStatus.class);
        STATUS_ORDER.put(OperationStatus.NONE, 0);
        STATUS_ORDER.put(OperationStatus.PREPARED, 1);
        STATUS_ORDER.put(OperationStatus.COMMITTED, 2);
        STATUS_ORDER.put(OperationStatus.EXECUTED, 3);
        STATUS_ORDER.put(OperationStatus.CHECKPOINTED, 4);

        this.entries = new ConcurrentHashMap<>();
        this.nextSeqNum = new AtomicLong(1L);
    }

    private int order(OperationStatus s) {
        return STATUS_ORDER.get(s);
    }

    // Leader path: allocate new seqNum and insert a NONE/PREPARED entry
    public long addOperation(ClientRequest request, Ballot ballot) {
        long seq = nextSeqNum.getAndIncrement();

        OperationLogEntry entry = new OperationLogEntry(
                request,
                ballot,
                OperationStatus.NONE
        );

        OperationLogEntry prev = entries.putIfAbsent(seq, entry);
        if (prev != null) {
            // Should not happen if seq numbers are strictly monotonic
            logger.warn("Seq {} already present when adding operation: {}", seq, prev);
        }
        return seq;
    }

    // Follower / generic: ensure entry exists for a specific seqNum
    // adds if null and return existing to be compared if present
    public OperationLogEntry ensureEntry(long seqNum, ClientRequest request, Ballot ballot) {
        return entries.compute(seqNum, (k, existing) -> {
            if (existing == null) {
                return new OperationLogEntry(request, ballot, OperationStatus.NONE);
            }
            // Keep existing request/ballot if already present
            return existing;
        });
    }

    public ClientRequest getRequest(long seqNum) {
        OperationLogEntry entry = entries.get(seqNum);
        return (entry != null) ? entry.request() : null;
    }

    public OperationStatus getStatus(long seqNum) {
        OperationLogEntry entry = entries.get(seqNum);
        return (entry != null) ? entry.status() : OperationStatus.NONE;
    }

    public boolean advanceStatus(long seqNum, OperationStatus newStatus) {
        OperationLogEntry result = entries.computeIfPresent(seqNum, (k, oldEntry) -> {
            OperationStatus current = oldEntry.status();
            // Do not go backwards or stay the same
            if (order(newStatus) <= order(current)) {
                return oldEntry;
            }
            return new OperationLogEntry(
                    oldEntry.request(),
                    oldEntry.ballot(),
                    newStatus
            );
        });

        return result != null && result.status() == newStatus;
    }

    public boolean compareAndSetStatus(long seqNum,
                                       OperationStatus expected,
                                       OperationStatus newStatus) {
        OperationLogEntry result = entries.computeIfPresent(seqNum, (k, oldEntry) -> {
            if (oldEntry.status() != expected) {
                return oldEntry; // no change
            }
            // Optional: enforce monotonicity here as well
            if (order(newStatus) < order(expected)) {
                logger.error("Attempt to move status backwards: {} -> {}", expected, newStatus);
                return oldEntry;
            }
            return new OperationLogEntry(
                    oldEntry.request(),
                    oldEntry.ballot(),
                    newStatus
            );
        });

        return result != null && result.status() == newStatus;
    }
}
