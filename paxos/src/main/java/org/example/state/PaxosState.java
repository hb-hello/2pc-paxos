package org.example.state;

import com.google.protobuf.MessageLite;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.messaging.ServerMessage;
import org.example.metrics.MetricsListener;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class PaxosState {
    private static final Logger logger = LogManager.getLogger(PaxosState.class);

    // Executor provided by ExecutorManager (named "state-manager-*" thread)
    private final ExecutorService stateExec;

    private final int serverId;
    private final Ballot ballot;
    private int leaderId;
    private Role role;
    private final OperationLog operationLog;
    private final ServerMessageTracker messageTracker;
    private final CheckpointManager checkpointManager;
    private AtomicLong latestCheckpointSeqSeen;
    private AtomicBoolean applyingCheckpoint;

    private boolean sentPrepare = false;
    private final Runnable onRoleChangeToBackup;

    public PaxosState(int serverId, ExecutorService stateExec, Consumer<ServerMessage<ClientRequest>> onNewClientRequest, Runnable onRoleChangeToBackup, MetricsListener metricsListener) {
        this.serverId = serverId;
        this.stateExec = stateExec;
        this.ballot = new Ballot(0, serverId);
        this.leaderId = -1; // No leader initially
        this.role = Role.CANDIDATE;
        this.messageTracker = new ServerMessageTracker();
        this.checkpointManager = new CheckpointManager();
        this.operationLog = new OperationLog(onNewClientRequest, checkpointManager, metricsListener);
        this.onRoleChangeToBackup = onRoleChangeToBackup;
        this.latestCheckpointSeqSeen = new AtomicLong(0L);
        this.applyingCheckpoint = new AtomicBoolean(false);
    }

    // Core scheduling helpers

    // Re-entrancy: rely on the named thread "state-manager-*"
    private boolean onStateThread() {
        String name = Thread.currentThread().getName();
//        logger.info("Current thread name: {}, on state thread? {}", name, name.startsWith("-state-manager"));
        return name != null && name.startsWith("-state-manager");
    }

    private <T> CompletableFuture<T> runAsync(Callable<T> task) {
        CompletableFuture<T> f = new CompletableFuture<>();
        stateExec.execute(() -> {
            try {
                f.complete(task.call());
            } catch (Throwable t) {
                f.completeExceptionally(t);
            }
        });
        return f;
    }

    // Overload for void-returning work
    private CompletableFuture<Void> runAsync(Runnable task) {
        return runAsync(() -> {
            task.run();
            return null;
        });
    }

    public <T> T runSync(Callable<T> task) {
        if (onStateThread()) {
            try {
//                logger.info("Running task synchronously on state thread");
                return task.call();
            } catch (Exception e) {
                throw wrap(e);
            }
        }
        // No timeout: block until completion
        try {
//            logger.info("Submitting task to state executor for synchronous execution as onStateThread was false");
            return runAsync(task).get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("State task interrupted", ie);
        } catch (ExecutionException ee) {
            throw wrap(ee.getCause());
        }
    }

    // Overload for void-returning work
    public void runSync(Runnable task) {
        runSync(() -> {
            task.run();
            return null;
        });
    }

    private RuntimeException wrap(Throwable t) {
        return (t instanceof RuntimeException re) ? re : new RuntimeException(t);
    }

    public int getServerId() {
        return serverId;
    }

    public int getLeaderId() {
        return runSync(() -> leaderId);
    }

    public Role getRole() {
        return runSync(() -> role);
    }

    public boolean isLeader() {
        return runSync(() -> role == Role.LEADER);
    }

    public boolean isBackup() {
        return runSync(() -> role == Role.BACKUP);
    }

    public boolean isCandidate() {
        return runSync(() -> role == Role.CANDIDATE);
    }

    public Ballot getBallot() {
        return runSync(() -> ballot);
    }

    public boolean updateBallot(Ballot newBallot) {
        return runSync(() -> {
            if (!ballot.isGreaterThan(newBallot)) {
                ballot.setBallot(newBallot);
                setSentPrepare(false);
                return true;
            }
            return false;
        });
    }

    public boolean hasSentPrepare() {
        return runSync(() -> sentPrepare);
    }

    public void setSentPrepare(boolean sentPrepare) {
        runSync(() -> {
            this.sentPrepare = sentPrepare;
        });
    }

    public void transitionToCandidate() {
        runSync(() -> {
            role = Role.CANDIDATE;
            ballot.incrementBallot(serverId);
            setSentPrepare(false);
            logger.info("Server {} initiating leader election with ballot {}", serverId, ballot);
        });
    }

    public boolean transitionToCandidate(Ballot newBallot) {
        return runSync(() -> {
            if (newBallot.isGreaterThan(ballot)) {
                role = Role.CANDIDATE;
                ballot.setBallot(newBallot);
                setSentPrepare(false);
                logger.info("Server {} transitioning to candidate role with ballot {}", serverId, ballot);
                return true;
            }
            return false;
        });
    }

    public boolean checkBallotAndTransitionToLeader(Ballot newBallot) {
        return runSync(() -> {
            if (isLeader()) return true;
            if (ballot.equals(newBallot)) {
                role = Role.LEADER;
                leaderId = serverId;
                logger.info("Server {} transitioned to LEADER with ballot {}", serverId, ballot);
                return true;
            }
            return false;
        });
    }

    public void transitionToBackup() {
        runSync(() -> {
            role = Role.BACKUP;
            leaderId = ballot.getServerId();
            onRoleChangeToBackup.run();
            logger.info("Server {} transitioned to BACKUP with leader ID {}", serverId, leaderId);
        });
    }

    public boolean checkBallotAndTransitionToBackup(Ballot newBallot) {
        return runSync(() -> {
            if (isBackup()) return true;
            if (updateBallot(newBallot)) {
                transitionToBackup();
                return true;
            }
            return false;
        });
    }

    public boolean trackMessageWithConsensus(ServerMessage<? extends MessageLite> message, int quorumRequired) {
        return messageTracker.addMessageWithConsensus(message, quorumRequired);
    }

    public OperationLog getOperationLog() {
        return operationLog;
    }

    public boolean hasRequestsWaitingToExecute() {
        return operationLog.hasRequestsWaitingToExecute();
    }

    public PromiseMessage getPromiseMessage() {
        return operationLog.getPromiseMessageWithLogs();
    }

    public long acceptRequest(ServerMessage<ClientRequest> request, Phase phase) {
        if (isLeader()) {
            Long seqNum = getSeqNumForRequest(request.getMessageId());
            if (seqNum != null) {
                if (operationLog.setOperationWithStatus(seqNum, request, ballot, OperationStatus.ACCEPTED, phase))
                    return seqNum;
            } else return operationLog.addOperationWithStatus(request, ballot, OperationStatus.ACCEPTED, phase);
        }
        return -1L;
    }

    public boolean acceptRequestWithSeqNum(ServerMessage<ClientRequest> request, Phase phase, long seqNum) {
        return operationLog.setOperationWithStatus(seqNum, request, ballot, OperationStatus.ACCEPTED, phase);
    }

    public boolean acceptRequest(ServerMessage<AcceptMessage> accept) {
        return operationLog.setOperationWithStatus(
                accept.payload().getSequenceNumber(),
                new ServerMessage<>(accept.payload().getRequest()),
                new Ballot(accept.payload().getBallot()),
                OperationStatus.ACCEPTED,
                accept.payload().getPhase()
        );
    }

    public boolean commitRequest(ServerMessage<CommitMessage> commit) {
        return operationLog.setOperationWithStatus(
                commit.payload().getSequenceNumber(),
                new ServerMessage<>(commit.payload().getRequest()),
                new Ballot(commit.payload().getBallot()),
                OperationStatus.COMMITTED,
                commit.payload().getPhase()
        );
    }

    public OperationLogEntry getLogEntry(long sequenceNumber) {
        return operationLog.getEntry(sequenceNumber);
    }

    public NewViewMessage getNewView() {
        return operationLog.getNewViewMessageWithLogs().toBuilder().setBallot(ballot.toProto()).build();
    }

    public String printOperationLog() {
        return operationLog.printLog();
    }

    public Long getSeqNumForRequest(String requestId) {
        return operationLog.getSeqNumForRequest(requestId);
    }

    public boolean addCheckpoint(long seqNum, String snapshot) {
        logger.info("Server {} attempting to add checkpoint at seqNum {}", serverId, seqNum);
        if (checkpointManager.addCheckpoint(seqNum, snapshot)) {
            operationLog.markCheckpointed(seqNum);
            logger.info("Server {} added checkpoint at seqNum {}", serverId, seqNum);
            return true;
        }
        return false;
    }

    public long getLatestCheckpointedSeqNum() {
        return checkpointManager.getLatestCheckpointedSeqNum();
    }

    public ServerMessage<CheckpointMessage> getLatestCheckpointMessage() {
        return new ServerMessage<>(checkpointManager.getLatestCheckpointMessage());
    }

    public ServerMessage<CheckpointMessage> getCheckpointMessage(long seqNum) {
        return new ServerMessage<>(checkpointManager.getCheckpointMessage(seqNum));
    }

    public void setLatestCheckpointSeqSeen(long seqNum) {
        runSync(() -> {
            if (latestCheckpointSeqSeen.get() < seqNum) this.latestCheckpointSeqSeen.set(seqNum);
        });
    }

    public long getLatestCheckpointSeqSeen() {
        return latestCheckpointSeqSeen.get();
    }

    public void setApplyingCheckpoint(boolean flag) {
        applyingCheckpoint.set(flag);
    }

    public boolean getApplyingCheckpoint() {
        return applyingCheckpoint.get();
    }
}
