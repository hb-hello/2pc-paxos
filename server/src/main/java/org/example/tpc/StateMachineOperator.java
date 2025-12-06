package org.example.tpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.config.Config;
import org.example.messaging.ServerMessage;
import org.example.persistence.KeyValueStore;
import org.example.state.OperationLog;
import org.example.state.OperationStatus;
import org.example.statemachine.BankStateMachine;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

public class StateMachineOperator {
    private static final Logger logger = LogManager.getLogger(StateMachineOperator.class);

    private final int serverId;

    private final ExecutorService stateMachineExecutor;   // single-threaded
    private final BankStateMachine stateMachine;
    private final OperationLog operationLog;
    private final ClientRequestTracker requestTracker;
    private final BiConsumer<String, Phase> onExecuted;
    private final Callable<Long> getLastCheckpointedSeqNum;
    private final BiConsumer<Long, String> onCheckpoint;
    private final Predicate<String> lockChecker;

    // WAL: client request -> before-image of mutated accounts
    private final ConcurrentMap<String, WalEntry> wal = new ConcurrentHashMap<>();

    // Local committed flag per client request (2PC decision on this shard)
    private final ConcurrentMap<String, Boolean> committedOrAborted = new ConcurrentHashMap<>();

    // Map from client request ID to sequence number (for checkpoint lookup)
    private final ConcurrentMap<String, Long> requestIdToSeqNum = new ConcurrentHashMap<>();

    // Highest sequence number for which we've received an ACK and locks have been released
    private final AtomicLong highestAckReceivedAndLocksReleasedSeqNum = new AtomicLong(0L);

    // Next sequence number that should be executed (in-order guarantee)
    private final AtomicLong nextToExecute = new AtomicLong(1L);

    // Highest seq any caller has requested us to execute up to.
    private final AtomicLong maxSeenTarget = new AtomicLong(0L);

    public StateMachineOperator(int serverId,
                                ExecutorService stateMachineExecutor,
                                KeyValueStore<Double> database,
                                OperationLog operationLog,
                                ClientRequestTracker requestTracker,
                                BiConsumer<String, Phase> onExecuted,
                                Callable<Long> getLastCheckpointedSeqNum,
                                BiConsumer<Long, String> onCheckpoint,
                                Predicate<String> lockChecker) {
        this.serverId = serverId;
        this.stateMachineExecutor = stateMachineExecutor;
        this.stateMachine = new BankStateMachine(database); // owns the DB-backed state machine
        this.operationLog = operationLog;
        this.requestTracker = requestTracker;
        this.onExecuted = onExecuted;
        this.getLastCheckpointedSeqNum = getLastCheckpointedSeqNum;
        this.onCheckpoint = onCheckpoint;
        this.lockChecker = lockChecker;
    }

    // Simple holder for WAL data
    private static final class WalEntry {
        private final Map<Integer, Double> beforeBalances;
        private volatile boolean committedOrAborted;

        private WalEntry(Map<Integer, Double> beforeBalances) {
            this.beforeBalances = beforeBalances;
            this.committedOrAborted = false;
        }

        public Map<Integer, Double> beforeBalances() {
            return Collections.unmodifiableMap(beforeBalances);
        }

        public boolean isCommittedOrAborted() {
            return committedOrAborted;
        }

        public void markCommittedOrAborted() {
            this.committedOrAborted = true;
        }
    }

    public Set<Integer> getModifiedAccounts() {
        return stateMachine.getModifiedAccounts();
    }

    /**
     * Asynchronously execute committed client requests from the Paxos operation log,
     * in-order and at-most-once, up to and including seqNum.
     * <p>
     * This method is safe to call repeatedly / concurrently; all work is serialized
     * on the stateExecutor.
     */
    public CompletableFuture<Void> execute(long seqNum, ExecutionMode mode) {

        try {
            if (seqNum <= getLastCheckpointedSeqNum.call()) {
                logger.info("Requested execution up to seqNum {} which is already checkpointed. No execution needed.", seqNum);
                return CompletableFuture.completedFuture(null);
            }
        } catch (Exception e) {
            logger.error("Error checking last checkpointed seq num", e);
            CompletableFuture<Void> failedFuture = new CompletableFuture<>();
        }

        logger.info("Scheduling execution up to seqNum {} with mode {}", seqNum, mode);

        // Remember the highest target ever requested.
        maxSeenTarget.updateAndGet(prev -> Math.max(prev, seqNum));

        CompletableFuture<Void> future = new CompletableFuture<>();
        stateMachineExecutor.execute(() -> {
            try {
                executeAsMuchAsPossible(mode);
                future.complete(null);
            } catch (Throwable t) {
                logger.error("Error executing up to seqNum {}", seqNum, t);
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    private void executeAsMuchAsPossible(ExecutionMode mode) {
        while (true) {
            long current = nextToExecute.get();
            long limit = maxSeenTarget.get();

            if (current == 0L) {
                nextToExecute.compareAndSet(0L, 1L);
                current = nextToExecute.get();
            }

            // Nothing more requested to be executed.
            if (current > limit) {
                return;
            }

            ServerMessage<ClientRequest> requestMessage = operationLog.getRequest(current);
            if (requestMessage == null) {
                logger.warn("No request found at seq {}. Stopping execution.", current);
                return;
            }

            ClientRequest request = requestMessage.payload();
            String requestId = requestMessage.getMessageId();

            if (Boolean.TRUE.equals(committedOrAborted.get(requestId))) {
                logger.info("Skipping seq {} request {} as it is already committed/aborted on this shard.",
                        current, requestId);
                nextToExecute.incrementAndGet();
                continue;
            }

            OperationStatus status = operationLog.getStatus(current);
            Phase phase = operationLog.getEntry(current).phase();

            boolean ready =
                    (status == OperationStatus.COMMITTED || status == OperationStatus.EXECUTED) &&
                            (phase == Phase.COMMIT || phase == Phase.INTRA_SHARD);

            if (!ready) {
                logger.info("Seq {} not ready to execute yet (status={}, phase={}). Stopping at this slot.",
                        current, status, phase);
                return;
            }

            if (status == OperationStatus.EXECUTED) {
                nextToExecute.incrementAndGet();
                continue;
            }

            try {
                if (current <= getLastCheckpointedSeqNum.call()) {
                    logger.info("Skipping execution for seq {} request {} as it is already included in a checkpoint.",
                            current, requestId);
                    nextToExecute.incrementAndGet();
                    continue;
                }
            } catch (Exception e) {
                logger.error("Error checking last checkpointed seq num", e);
                return;
            }

            WalEntry walEntry = buildWalEntry(request, mode);
            if (walEntry != null) {
                wal.put(requestId, walEntry);
            }

            OperationResult result = stateMachine.execute(request.getOperation(), mode);

            logger.info("Executed seq {} for request {} resultCase={}",
                    current, requestId, result.getResultCase());

            ClientReply reply = ClientReply.newBuilder()
                    .setResult(result)
                    .setSenderId(serverId)
                    .setRequestId(request.getRequestId())
                    .build();
            ServerMessage<ClientReply> replyMessage = new ServerMessage<>(reply);
            requestTracker.storeReply(requestMessage, replyMessage);

            boolean markedInLog = operationLog.compareAndSetStatus(
                    current, OperationStatus.COMMITTED, OperationStatus.EXECUTED);

            logger.info("Marked seq {} request {} as EXECUTED in log: {}",
                    current, requestId, markedInLog);

            if (!markedInLog) {
                logger.error("Failed to mark seq {} request {} as EXECUTED in operation log",
                        current, requestId);
                return;
            }

            nextToExecute.incrementAndGet();
            // Record the mapping from request ID to sequence number for checkpoint lookup
            requestIdToSeqNum.put(requestId, current);
            onExecuted.accept(requestId, phase);

            try {
                if (highestAckReceivedAndLocksReleasedSeqNum.get() <= getLastCheckpointedSeqNum.call() && current % Config.getCheckpointInterval() == 0) {
                    onCheckpoint.accept(current, stateMachine.snapshot());
                }
            } catch (Exception e) {
                logger.error("Error checking last checkpointed seq num during execution", e);
                return;
            }
        }
    }


//    private void executeUpTo(long targetSeq, ExecutionMode mode) {
//        while (true) {
//            long current = nextToExecute.get();
//            if (current > targetSeq) {
//                logger.warn("Skipping execution for seq {} as we're already at seq {}", targetSeq, current);
//                return;
//            }
//
//            ServerMessage<ClientRequest> requestMessage = operationLog.getRequest(current);
//            if (requestMessage == null) {
//                logger.warn("No request found at seq {}. Stopping execution.", current);
//                return;
//            }
//            ClientRequest request = requestMessage.payload();
//            String requestId = requestMessage.getMessageId();
//
//            // Skip if already marked committed/executed
//            if (Boolean.TRUE.equals(committedOrAborted.get(requestId))) {
//                nextToExecute.incrementAndGet();
//                logger.info("Skipping execution for seq {} request {} as it is already committed or aborted on this shard.", current, requestId);
//                continue;
//            }
//
//            OperationStatus status = operationLog.getStatus(current); // NONE/PREPARED/COMMITTED/EXECUTED/CHECKPOINTED
//            Phase phase = operationLog.getEntry(current).phase();
//            if (status != OperationStatus.COMMITTED && status != OperationStatus.EXECUTED && (phase == Phase.PREPARE || phase == Phase.INTRA_SHARD)) {
//                // Not ready to execute in log order yet.
//                logger.info("Skipping execution at seq {} for request {} as status is {} and phase is {}.",
//                        current, requestMessage.getMessageId(), status, phase);
//                return;
//            }
//
//            if (status == OperationStatus.EXECUTED) {
////                logger.info("Request {} at seq {} is already EXECUTED. Skipping.", requestMessage.getMessageId(), current);
//                nextToExecute.incrementAndGet();
//                continue;
//            }
//
//            // Build WAL before applying the operation
//            WalEntry entry = buildWalEntry(request, mode);
//            if (entry != null) {
//                wal.put(requestId, entry);
//            }
//
//            // Execute the operation on the state machine
//            OperationResult result =
//                    stateMachine.execute(request.getOperation(), mode);
//
//            logger.info("Executed seq {} for request {} resultCase={}",
//                    current, requestMessage.getMessageId(), result.getResultCase());
//
//            // Build and store client reply in the tracker
//            ClientReply reply = ClientReply.newBuilder()
//                    .setResult(result)
//                    .setSenderId(serverId)
//                    .setClientId(request.getClientId())
//                    .setTimestamp(request.getTimestamp())
//                    .build();
//            ServerMessage<ClientReply> replyMessage = new ServerMessage<>(reply);
//            requestTracker.storeReply(requestMessage, replyMessage);
//
//            // Mark as EXECUTED in the Paxos log atomically to ensure at-most-once
//            boolean markedInLog = operationLog.compareAndSetStatus(
//                    current, OperationStatus.COMMITTED, OperationStatus.EXECUTED);
//
//            logger.info("Marked seq {} request {} as EXECUTED in log: {}", current, requestMessage.getMessageId(), markedInLog);
//
//            if (!markedInLog) {
//                logger.error("Failed to mark seq {} request {} as EXECUTED in operation log", current, requestMessage.getMessageId());
//                return;
//            }
//
//            nextToExecute.incrementAndGet();
//
//            // Notify listener that the operation has been executed
//            onExecuted.accept(requestMessage.getMessageId(), operationLog.getEntry(current).phase());
//        }
//    }

    /**
     * For non-read-only operations, capture the pre-operation balances of all
     * accounts that may be mutated, as a before-image.
     */
    private WalEntry buildWalEntry(ClientRequest request, ExecutionMode mode) {
        Operation op = request.getOperation();
        switch (op.getOpCase()) {
            case TRANSFER -> {
                int sender = op.getTransfer().getSender();
                int receiver = op.getTransfer().getReceiver();
                Map<Integer, Double> before = new HashMap<>(2);
                if (mode == ExecutionMode.BOTH || mode == ExecutionMode.SENDER) {
                    before.put(sender, stateMachineBalance(sender));
                }
                if (mode == ExecutionMode.BOTH || mode == ExecutionMode.RECEIVER) {
                    before.put(receiver, stateMachineBalance(receiver));
                }
                if (before.isEmpty()) {
                    return null;
                }
                return new WalEntry(before);
            }
            case BALANCE_REQUEST -> {
                return null;
            }
            case OP_NOT_SET -> throw new IllegalArgumentException("Operation.op not set");
        }
        throw new IllegalStateException("Unhandled opCase: " + op.getOpCase());
    }

    // Helper to reuse the same lookup path BankStateMachine uses.
    private double stateMachineBalance(int accountId) {
        Operation balanceOp = Operation.newBuilder()
                .setBalanceRequest(
                        org.example.BalanceRequest.newBuilder().setAccountId(accountId).build()
                ).build();
        OperationResult res =
                stateMachine.execute(balanceOp, ExecutionMode.BOTH);
        return res.getBalance();
    }

    /**
     * Undo the effects of the given sequence number using its WAL before-image.
     * Intended for 2PC ABORT on this shard.
     */
    public CompletableFuture<Void> undo(String requestId) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        stateMachineExecutor.execute(() -> {
            try {
                WalEntry entry = wal.remove(requestId);
                if (entry != null && !entry.isCommittedOrAborted()) {
                    for (Map.Entry<Integer, Double> e : entry.beforeBalances().entrySet()) {
                        int accountId = e.getKey();
                        double balance = e.getValue();
                        stateMachine.restoreBalance(accountId, balance);
                    }
                }
                committedOrAborted.remove(requestId);
                f.complete(null);
            } catch (Throwable t) {
                logger.error("Error undoing request: {}", requestId, t);
                f.completeExceptionally(t);
            }
        });
        return f;
    }

    /**
     * Mark the given sequence as committed on this shard and flush its WAL entry.
     * After this call, the operation is durable in the main DB and cannot be undone.
     */
    public CompletableFuture<Void> markCommittedOrAborted(String requestId) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        stateMachineExecutor.execute(() -> {
            try {
                committedOrAborted.put(requestId, true);
                WalEntry entry = wal.remove(requestId);
                if (entry != null) {
                    entry.markCommittedOrAborted();
                }
                f.complete(null);
            } catch (Throwable t) {
                logger.error("Error marking request committed or aborted : {}", requestId, t);
                f.completeExceptionally(t);
            }
        });
        return f;
    }

    /**
     * Execute a read-only operation on the calling thread.
     */
    public OperationResult executeReadOnly(Operation operation) {
        switch (operation.getOpCase()) {
            case BALANCE_REQUEST -> {
                // BankStateMachine.executeOperation for BalanceRequest just reads from DB.
                return stateMachine.execute(operation, ExecutionMode.BOTH);
            }
            case TRANSFER -> throw new IllegalArgumentException(
                    "executeReadOnly does not support TRANSFER operations");
            case OP_NOT_SET -> throw new IllegalArgumentException("Operation.op not set");
        }
        throw new IllegalStateException("Unhandled opCase: " + operation.getOpCase());
    }

    /**
     * Try to trigger a checkpoint when we receive an ACK from a participant cluster in 2PC.
     * A checkpoint is triggered if:
     * 1. We have a recorded sequence number for this request
     * 2. All requests in the interval (highestAckReceivedAndLocksReleasedSeqNum+1 to nextCheckpointTarget)
     *    have received their ACKs (if cross-shard) and have released their locks
     *
     * @param requestMessage the client request for which we received an ACK
     * @return true if a checkpoint was triggered, false otherwise
     */
    public boolean tryCheckpoint(ServerMessage<ClientRequest> requestMessage) {
        String requestId = requestMessage.getMessageId();
        Long seqNum = requestIdToSeqNum.get(requestId);

        if (seqNum == null) {
            logger.warn("No sequence number found for request {} - cannot consider for checkpoint", requestId);
            return false;
        }

        int checkpointInterval = Config.getCheckpointInterval();

        long latestCheckpoint;
        try {
            latestCheckpoint = getLastCheckpointedSeqNum.call();
        } catch (Exception e) {
            logger.error("Error getting last checkpointed seq num for tryCheckpoint", e);
            return false;
        }

        // Find the next checkpoint target (the next multiple of checkpointInterval after latestCheckpoint)
        long nextCheckpointTarget = latestCheckpoint < 0
                ? checkpointInterval
                : ((latestCheckpoint / checkpointInterval) + 1) * checkpointInterval;

        // Check all requests in the interval (highestAckReceivedAndLocksReleasedSeqNum+1 to nextCheckpointTarget)
        // Each request must have:
        // - ACK received (if cross-shard with SENDER mode)
        // - Locks released
        long intervalStart = Math.max(1, highestAckReceivedAndLocksReleasedSeqNum.get() + 1);
        for (long seq = intervalStart; seq <= nextCheckpointTarget; seq++) {
            ServerMessage<ClientRequest> req = operationLog.getRequest(seq);
            if (req == null) {
                // If we haven't even seen this seq yet, we can't checkpoint
                logger.debug("Cannot checkpoint at seq {} - request at seq {} not found yet",
                        nextCheckpointTarget, seq);
                return false;
            }

            var entry = operationLog.getEntry(seq);
            if (entry == null) {
                logger.debug("Cannot checkpoint at seq {} - entry at seq {} not found in operation log",
                        nextCheckpointTarget, seq);
                return false;
            }

            String reqId = req.getMessageId();

            // Check if locks have been released for this request
            if (!lockChecker.test(reqId)) {
                logger.debug("Cannot checkpoint at seq {} - request {} at seq {} still holds locks",
                        nextCheckpointTarget, reqId, seq);
                return false;
            }

            Phase phase = entry.phase();
            ExecutionMode mode = requestTracker.getExecutionMode(req);

            // For cross-shard requests where we are the sender, check if ACK has been received
            if (phase != Phase.INTRA_SHARD && mode != ExecutionMode.RECEIVER) {
                if (!requestTracker.isAckReceived(req)) {
                    logger.debug("Cannot checkpoint at seq {} - request {} at seq {} (phase={}) has not received ACK yet",
                            nextCheckpointTarget, reqId, seq, phase);
                    return false;
                }
            }

            // This seq is ready - update highestAckReceivedAndLocksReleasedSeqNum
            final long currentSeq = seq;
            highestAckReceivedAndLocksReleasedSeqNum.updateAndGet(prev -> Math.max(prev, currentSeq));
        }

        logger.info("Triggering checkpoint at seq {} (latestCheckpoint={}, interval={})",
                nextCheckpointTarget, latestCheckpoint, checkpointInterval);
        onCheckpoint.accept(nextCheckpointTarget, stateMachine.snapshot());
        return true;
    }

    public long getHighestAckReceivedSeqNum() {
        return highestAckReceivedAndLocksReleasedSeqNum.get();
    }

    /**
     * Apply a checkpoint snapshot to restore the state machine state.
     * This is used during recovery or when catching up from a checkpoint.
     *
     * @param stateSnapshot the serialized state snapshot to apply
     * @return a future that completes when the snapshot has been applied
     */
    public CompletableFuture<Void> applyCheckpoint(long seqNum, String stateSnapshot) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        stateMachineExecutor.execute(() -> {
            try {
                stateMachine.applySnapshot(stateSnapshot);
                logger.info("Applied checkpoint snapshot to state machine");
                onCheckpoint.accept(seqNum, stateSnapshot);
                f.complete(null);
            } catch (Throwable t) {
                logger.error("Error applying checkpoint snapshot", t);
                f.completeExceptionally(t);
            }
        });
        return f;
    }

    public void registerRequestSeqNum(String requestId, long seqNum) {
        requestIdToSeqNum.put(requestId, seqNum);
    }

    public Long getSeqNumForRequest(String requestId) {
        return requestIdToSeqNum.get(requestId);
    }
}
