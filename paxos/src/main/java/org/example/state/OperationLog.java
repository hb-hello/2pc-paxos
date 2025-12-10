package org.example.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.config.Config;
import org.example.messaging.ServerMessage;
import org.example.metrics.MetricsListener;

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

    private final Consumer<ServerMessage<ClientRequest>> onNewClientRequest;

    private final CheckpointManager checkpointManager;
    private final MetricsListener metricsListener;

    // Map from client request ID to sequence number (for checkpoint lookup)
    private final ConcurrentMap<String, Long> requestIdToSeqNum = new ConcurrentHashMap<>();

    public OperationLog(Consumer<ServerMessage<ClientRequest>> onNewClientRequest, CheckpointManager checkpointManager, MetricsListener metricsListener) {
        this.STATUS_ORDER = new EnumMap<>(OperationStatus.class);
        STATUS_ORDER.put(OperationStatus.NONE, 0);
        STATUS_ORDER.put(OperationStatus.ACCEPTED, 1);
        STATUS_ORDER.put(OperationStatus.COMMITTED, 2);
        STATUS_ORDER.put(OperationStatus.EXECUTED, 3);
        STATUS_ORDER.put(OperationStatus.CHECKPOINTED, 4);

        this.entries = new ConcurrentHashMap<>();
        this.nextSeqNum = new AtomicLong(1L);

        this.onNewClientRequest = onNewClientRequest;

        this.checkpointManager = checkpointManager;
        this.metricsListener = metricsListener;
    }

    private int order(OperationStatus s) {
        return STATUS_ORDER.get(s);
    }

    public long getNextSeqNum() {
        return nextSeqNum.get();
    }

    public long addOperation(ServerMessage<ClientRequest> request, Ballot ballot) {
        return addOperationWithStatus(request, ballot, OperationStatus.NONE, Phase.PREPARE);
    }

    public long addOperationWithStatus(ServerMessage<ClientRequest> request, Ballot ballot, OperationStatus status, Phase phase) {
        long seq = nextSeqNum.getAndIncrement();

        // Reject if seq is at or below the latest checkpoint
        long latestCp = checkpointManager.getLatestCheckpointedSeqNum();
        if (seq <= latestCp) {
            logger.warn("Rejecting addOperationWithStatus: seqNum {} <= latestCheckpoint {}", seq, latestCp);
            return -1L;
        }

        OperationLogEntry entry = new OperationLogEntry(
                request,
                ballot,
                status,
                phase
        );

        long now = System.nanoTime();
        OperationLogEntry prev = entries.putIfAbsent(seq, entry);
        if (prev != null) {
            // Should not happen if seq numbers are strictly monotonic
            logger.warn("Seq {} already present when adding operation: {}", seq, prev);
        }
        // Record the mapping from request ID to sequence number
        requestIdToSeqNum.put(request.getMessageId(), seq);
        metricsListener.onStatusTransition(seq, OperationStatus.NONE, status, now);
        return seq;
    }

    public ServerMessage<ClientRequest> getRequest(long seqNum) {
        // Reject if seqNum is at or below the latest checkpoint
        if (seqNum <= checkpointManager.getLatestCheckpointedSeqNum()) {
            return null;
        }
        OperationLogEntry entry = entries.get(seqNum);
        return (entry != null) ? entry.request() : null;
    }

    public OperationLogEntry getEntry(long seqNum) {
        // Reject if seqNum is at or below the latest checkpoint
        if (seqNum <= checkpointManager.getLatestCheckpointedSeqNum()) {
            return null;
        }
        return entries.get(seqNum);
    }

    public OperationStatus getStatus(long seqNum) {
        // Reject if seqNum is at or below the latest checkpoint
        if (seqNum <= checkpointManager.getLatestCheckpointedSeqNum()) {
            return null;
        }
        OperationLogEntry entry = entries.get(seqNum);
        return (entry != null) ? entry.status() : OperationStatus.NONE;
    }

    public boolean advanceStatus(long seqNum, OperationStatus newStatus) {
        // Reject if seqNum is at or below the latest checkpoint
        if (seqNum <= checkpointManager.getLatestCheckpointedSeqNum()) {
            return false;
        }
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
        // Reject if seqNum is at or below the latest checkpoint
        if (seqNum <= checkpointManager.getLatestCheckpointedSeqNum()) {
            return false;
        }
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

        if (result != null && result.status() == newStatus) {
            long now = System.nanoTime();
            metricsListener.onStatusTransition(seqNum, expected, newStatus, now);
            return true;
        }

        return false;
    }

    public boolean setOperationWithStatus(long seqNum,
                                          ServerMessage<ClientRequest> request,
                                          Ballot ballot,
                                          OperationStatus status,
                                          Phase phase) {
        // Reject if seqNum is at or below the latest checkpoint
        if (seqNum <= checkpointManager.getLatestCheckpointedSeqNum()) {
            logger.warn("Rejecting setOperationWithStatus: seqNum {} <= latestCheckpoint", seqNum);
            return false;
        }
        AtomicBoolean updated = new AtomicBoolean(false);
        AtomicBoolean newRequest = new AtomicBoolean(false);
        entries.compute(seqNum, (k, oldEntry) -> {
            if (oldEntry == null) {
                updated.set(true);
                onNewClientRequest.accept(request);
                newRequest.set(true);
                return new OperationLogEntry(request, ballot, status, phase);
            }

            Ballot existingBallot = oldEntry.ballot();
            if (existingBallot != null && existingBallot.isGreaterThan(ballot)) {
                logger.warn("Cannot overwrite entry at seqNum {} with lower ballot: existing {}, new {}",
                        seqNum, existingBallot, ballot);
                return oldEntry;
            }

            if (order(status) <= order(oldEntry.status()) && phase == oldEntry.phase()) {
                logger.warn("Status/phase change for seqNum {} not allowed: existing status {}, phase {}; new status {}, phase {}",
                        seqNum, oldEntry.status(), oldEntry.phase(), status, phase);
                return oldEntry;
            }

            if (phase != oldEntry.phase() && (oldEntry.phase() == Phase.COMMIT || oldEntry.phase() == Phase.ABORT)) {
                logger.warn("Cannot change phase from {} to {} for seqNum {}", oldEntry.phase(), phase, seqNum);
                return oldEntry;
            }

            updated.set(true);
            long now = System.nanoTime();
            metricsListener.onStatusTransition(seqNum, oldEntry.status(), status, now);
            return new OperationLogEntry(request, ballot, status, phase);
        });

        if (updated.get()) {
            nextSeqNum.accumulateAndGet(seqNum + 1, Math::max);
            // Record the mapping from request ID to sequence number
            requestIdToSeqNum.put(request.getMessageId(), seqNum);
        }

        return updated.get();
    }

    public boolean hasRequestsWaitingToExecute() {
        for (long seqNum = checkpointManager.getLatestCheckpointedSeqNum(); seqNum < nextSeqNum.get(); seqNum++) {
            OperationStatus status = entries.get(seqNum).status();
            Phase phase = entries.get(seqNum).phase();
            if (phase == Phase.PREPARE && (status != OperationStatus.EXECUTED)) {
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
        for (long i = Math.max(1L, checkpointManager.getLatestCheckpointedSeqNum() - Config.getCheckpointInterval()); i <= seqNum; i++) {
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
            nextSeqNum.set(Math.max(nextSeqNum.get(), seqNum + 1));
        }
    }

    public PromiseMessage getPromiseMessageWithLogs() {

        PromiseMessage.Builder promiseBuilder = PromiseMessage.newBuilder();

        for (long i = Math.max(1L, checkpointManager.getLatestCheckpointedSeqNum()); i < nextSeqNum.get(); i++) {
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

        for (long i = Math.max(1L, checkpointManager.getLatestCheckpointedSeqNum()); i < nextSeqNum.get(); i++) {
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

    public String printLog() {
        StringBuilder sb = new StringBuilder();
        for (long i = 1L; i < nextSeqNum.get(); i++) {
            OperationLogEntry entry = entries.get(i);
            sb.append("Seq ").append(i)
              .append(": Status=").append(entry.status())
              .append(", Phase=").append(entry.phase())
              .append(", Ballot=").append(entry.ballot())
              .append(", Request=").append(entry.request().getMessageId())
              .append("\n");
        }
        return sb.toString();
    }

    /**
     * Get the sequence number for a given request ID.
     *
     * @param requestId the client request ID
     * @return the sequence number, or null if not found
     */
    public Long getSeqNumForRequest(String requestId) {
        return requestIdToSeqNum.get(requestId);
    }
}
