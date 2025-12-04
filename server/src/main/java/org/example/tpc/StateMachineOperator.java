package org.example.tpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.messaging.ServerMessage;
import org.example.persistence.KeyValueStore;
import org.example.state.OperationLog;
import org.example.state.OperationStatus;
import org.example.statemachine.BankStateMachine;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

public class StateMachineOperator {
    private static final Logger logger = LogManager.getLogger(StateMachineOperator.class);

    private final int serverId;

    private final ExecutorService stateMachineExecutor;   // single-threaded
    private final BankStateMachine stateMachine;
    private final OperationLog operationLog;
    private final ClientRequestTracker requestTracker;
    private final BiConsumer<ServerMessage<ClientRequest>, Phase> onExecuted;

    // WAL: client request -> before-image of mutated accounts
    private final ConcurrentMap<String, WalEntry> wal = new ConcurrentHashMap<>();

    // Local committed flag per client request (2PC decision on this shard)
    private final ConcurrentMap<String, Boolean> committedOrAborted = new ConcurrentHashMap<>();

    // Next sequence number that should be executed (in-order guarantee)
    private final AtomicLong nextToExecute = new AtomicLong(1L);

    public StateMachineOperator(int serverId,
                                ExecutorService stateMachineExecutor,
                                KeyValueStore<Double> database,
                                OperationLog operationLog,
                                ClientRequestTracker requestTracker,
                                BiConsumer<ServerMessage<ClientRequest>, Phase> onExecuted) {
        this.serverId = serverId;
        this.stateMachineExecutor = stateMachineExecutor;
        this.stateMachine = new BankStateMachine(database); // owns the DB-backed state machine
        this.operationLog = operationLog;
        this.requestTracker = requestTracker;
        this.onExecuted = onExecuted;
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
        CompletableFuture<Void> future = new CompletableFuture<>();
        stateMachineExecutor.execute(() -> {
            try {
                executeUpTo(seqNum, mode);
                future.complete(null);
            } catch (Throwable t) {
                logger.error("Error executing up to seqNum {}", seqNum, t);
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    private void executeUpTo(long targetSeq, ExecutionMode mode) {
        while (true) {
            long current = nextToExecute.get();
            if (current > targetSeq) {
                return;
            }

            ServerMessage<ClientRequest> requestMessage = operationLog.getRequest(current);
            if (requestMessage == null) {
                return;
            }
            ClientRequest request = requestMessage.payload();
            String requestId = requestMessage.getMessageId();

            // Skip if already marked committed/executed
            if (Boolean.TRUE.equals(committedOrAborted.get(requestId))) {
                nextToExecute.incrementAndGet();
                continue;
            }

            OperationStatus status = operationLog.getStatus(current); // NONE/PREPARED/COMMITTED/EXECUTED/CHECKPOINTED
            Phase phase = operationLog.getEntry(current).phase();
            if (status != OperationStatus.COMMITTED && status != OperationStatus.EXECUTED && (phase == Phase.PREPARE || phase == Phase.INTRA_SHARD)) {
                // Not ready to execute in log order yet.
                return;
            }

            if (status == OperationStatus.EXECUTED) {
                nextToExecute.incrementAndGet();
                continue;
            }

            // Build WAL before applying the operation
            WalEntry entry = buildWalEntry(request, mode);
            if (entry != null) {
                wal.put(requestId, entry);
            }

            // Execute the operation on the state machine
            OperationResult result =
                    stateMachine.execute(request.getOperation(), mode);

            logger.info("Executed seq {} for client {} resultCase={}",
                    current, request.getClientId(), result.getResultCase());

            // Build and store client reply in the tracker
            ClientReply reply = ClientReply.newBuilder()
                    .setResult(result)
                    .setSenderId(serverId)
                    .setClientId(request.getClientId())
                    .setTimestamp(request.getTimestamp())
                    .build();
            ServerMessage<ClientReply> replyMessage = new ServerMessage<>(reply);
            requestTracker.storeReply(requestMessage, replyMessage);

            // Mark as EXECUTED in the Paxos log atomically to ensure at-most-once
            operationLog.compareAndSetStatus(
                    current, OperationStatus.COMMITTED, OperationStatus.EXECUTED);

            nextToExecute.incrementAndGet();

            // Notify listener that the operation has been executed
            onExecuted.accept(requestMessage, operationLog.getEntry(current).phase());
        }
    }

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
}
