package org.example.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.messaging.ServerMessage;

import java.util.EnumMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class OperationLog {
    private static final Logger logger = LogManager.getLogger(OperationLog.class);

    private final EnumMap<OperationStatus, Integer> STATUS_ORDER;
    private final ConcurrentMap<Long, OperationLogEntry> entries;
    private final AtomicLong nextSeqNum;
    private final AtomicLong lastCheckpointSeqNum;

    private final Consumer<ServerMessage<ClientRequest>> onNewClientRequest;

    //map of OperationLogEntry keyed by seqNum

    public OperationLog(Consumer<ServerMessage<ClientRequest>> onNewClientRequest) {
        this.STATUS_ORDER = new EnumMap<>(OperationStatus.class);
        STATUS_ORDER.put(OperationStatus.NONE, 0);
        STATUS_ORDER.put(OperationStatus.ACCEPTED, 1);
        STATUS_ORDER.put(OperationStatus.COMMITTED, 2);
        STATUS_ORDER.put(OperationStatus.EXECUTED, 3);
        STATUS_ORDER.put(OperationStatus.CHECKPOINTED, 4);

        this.entries = new ConcurrentHashMap<>();
        this.nextSeqNum = new AtomicLong(1L);
        this.lastCheckpointSeqNum = new AtomicLong(0L);

        this.onNewClientRequest = onNewClientRequest;
    }

    private int order(OperationStatus s) {
        return STATUS_ORDER.get(s);
    }

    public long getNextSeqNum() {
        return nextSeqNum.get();
    }

    // Leader path: allocate new seqNum and insert a NONE/PREPARED entry
    public long addOperation(ServerMessage<ClientRequest> request, Ballot ballot) {
        return addOperationWithStatus(request, ballot, OperationStatus.NONE, Phase.PREPARE);
    }

    public long addOperationWithStatus(ServerMessage<ClientRequest> request, Ballot ballot, OperationStatus status, Phase phase) {
        long seq = nextSeqNum.getAndIncrement();

        OperationLogEntry entry = new OperationLogEntry(
                request,
                ballot,
                status,
                phase
        );

        OperationLogEntry prev = entries.putIfAbsent(seq, entry);
        if (prev != null) {
            // Should not happen if seq numbers are strictly monotonic
            logger.warn("Seq {} already present when adding operation: {}", seq, prev);
        }
        return seq;
    }

    public ServerMessage<ClientRequest> getRequest(long seqNum) {
        OperationLogEntry entry = entries.get(seqNum);
        return (entry != null) ? entry.request() : null;
    }

    public OperationLogEntry getEntry(long seqNum) {
        return entries.get(seqNum);
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
                    newStatus,
                    oldEntry.phase()
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
                    newStatus,
                    oldEntry.phase()
            );
        });

        return result != null && result.status() == newStatus;
    }

    public boolean setOperationWithStatus(long seqNum,
                                          ServerMessage<ClientRequest> request,
                                          Ballot ballot,
                                          OperationStatus status,
                                          Phase phase) {
        AtomicBoolean updated = new AtomicBoolean(false);
        AtomicBoolean newRequest = new AtomicBoolean(false);
        entries.compute(seqNum, (k, oldEntry) -> {
            if (oldEntry == null) {
                updated.set(true);
                newRequest.set(true);
                return new OperationLogEntry(request, ballot, status, phase);
            }

            Ballot existingBallot = oldEntry.ballot();
            if (existingBallot != null && existingBallot.isGreaterThan(ballot)) {
                return oldEntry;
            }

            updated.set(true);
            return new OperationLogEntry(request, ballot, status, phase);
        });

        if (newRequest.get()) {
            onNewClientRequest.accept(request);
        }

        return updated.get();
    }

    public boolean hasRequestsWaitingToExecute() {
        for (long seqNum : entries.keySet()) {
            OperationStatus status = entries.get(seqNum).status();
            if (status == OperationStatus.COMMITTED || status == OperationStatus.ACCEPTED || status == OperationStatus.NONE) {
                logger.info("Waiting to execute : Found request at seqNum {} with status {}", seqNum, status);
                return true;
            }
        }
        logger.info("No requests waiting to execute");
        return false;
    }

    public void markCheckpointed(long seqNum) {
        if (seqNum <= 0) {
            return;
        }
        for (long i = Math.max(1L, lastCheckpointSeqNum.get()); i <= seqNum; i++) {
            entries.computeIfPresent(i, (k, oldEntry) -> {
                if (oldEntry.status() == OperationStatus.CHECKPOINTED) {
                    return oldEntry;
                }
                return new OperationLogEntry(
                        oldEntry.request(),
                        oldEntry.ballot(),
                        OperationStatus.CHECKPOINTED,
                        oldEntry.phase()
                );
            });
        }
        lastCheckpointSeqNum.updateAndGet(prev -> Math.max(prev, seqNum));
    }

    public PromiseMessage getPromiseMessageWithLogs() {

        PromiseMessage.Builder promiseBuilder = PromiseMessage.newBuilder();

        for (long i = Math.max(1L, lastCheckpointSeqNum.get()); i < nextSeqNum.get(); i++) {
            OperationLogEntry entry = entries.get(i);
            OperationStatus status = entry.status();
            org.example.Ballot ballot = entry.ballot().toProto();
            Phase phase = entry.phase();
            ClientRequest request = entry.request().payload();
            if (status == OperationStatus.ACCEPTED) {
                AcceptMessage message = AcceptMessage.newBuilder()
                        .setBallot(ballot)
                        .setPhase(phase)
                        .setRequest(request)
                        .setSequenceNumber(i)
                        .build();
                promiseBuilder.addAcceptLog(message);
            } else if (status == OperationStatus.COMMITTED || status == OperationStatus.EXECUTED) {
                CommitMessage message = CommitMessage.newBuilder()
                        .setBallot(ballot)
                        .setPhase(phase)
                        .setRequest(request)
                        .setSequenceNumber(i)
                        .build();
                promiseBuilder.addCommitLog(message);
            }
        }

        return promiseBuilder.build();
    }

    public NewViewMessage getNewViewMessageWithLogs() {

        NewViewMessage.Builder newViewBuilder = NewViewMessage.newBuilder();

        for (long i = Math.max(1L, lastCheckpointSeqNum.get()); i < nextSeqNum.get(); i++) {
            OperationLogEntry entry = entries.get(i);
            OperationStatus status = entry.status();
            org.example.Ballot ballot = entry.ballot().toProto();
            Phase phase = entry.phase();
            ClientRequest request = entry.request().payload();
            if (status == OperationStatus.ACCEPTED) {
                AcceptMessage message = AcceptMessage.newBuilder()
                        .setBallot(ballot)
                        .setPhase(phase)
                        .setRequest(request)
                        .setSequenceNumber(i)
                        .build();
                newViewBuilder.addAcceptLog(message);
            } else if (status == OperationStatus.COMMITTED || status == OperationStatus.EXECUTED) {
                CommitMessage message = CommitMessage.newBuilder()
                        .setBallot(ballot)
                        .setPhase(phase)
                        .setRequest(request)
                        .setSequenceNumber(i)
                        .build();
                newViewBuilder.addCommitLog(message);
            }
        }

        return newViewBuilder.build();
    }
}
