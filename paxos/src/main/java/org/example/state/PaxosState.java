package org.example.state;

import com.google.protobuf.MessageLite;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.NewViewMessage;
import org.example.messaging.ServerMessage;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

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

    private boolean sentPrepare = false;

    public PaxosState(int serverId, ExecutorService stateExec) {
        this.serverId = serverId;
        this.stateExec = stateExec;
        this.ballot = new Ballot(0, serverId);
        this.leaderId = -1; // No leader initially
        this.role = Role.CANDIDATE;
        this.operationLog = new OperationLog();
        this.messageTracker = new ServerMessageTracker();
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

    public Ballot getBallot() {
        return runSync(() -> ballot);
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
            if (!sentPrepare) {
                role = Role.CANDIDATE;
                ballot.incrementBallot(serverId);
                logger.info("Server {} initiating leader election with ballot {}", serverId, ballot);
            }
        });
    }

    public boolean checkBallotAndTransitionToLeader(Ballot newBallot) {
        return runSync(() -> {
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
            logger.info("Server {} transitioned to BACKUP with leader ID {}", serverId, leaderId);
        });
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

    public boolean trackMessageWithConsensus(ServerMessage<? extends MessageLite> message, int quorumRequired) {
        return messageTracker.addMessageWithConsensus(message, quorumRequired);
    }

    public NewViewMessage constructNewView() {
        // take a copy of all promise messages for current ballot
        // take a snapshot of pending client requests
        return NewViewMessage.newBuilder().setBallot(ballot.toProto()).build();
    }
}
