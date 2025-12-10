package org.example.tpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.Operation;
import org.example.persistence.KeyValueStore;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LockManager {
    private static final Logger logger = LogManager.getLogger(LockManager.class);

    // accountId -> owning transactionId
    private final ConcurrentMap<Integer, String> locks = new ConcurrentHashMap<>();

    // Set of all transaction IDs that currently hold at least one lock
    private final Set<String> transactionsWithLocks = ConcurrentHashMap.newKeySet();

    /**
     * Try to acquire a lock on accountId for txId.
     * Returns true if:
     *  - the lock was free and is now acquired by txId, or
     *  - the lock was already held by the same txId (idempotent).
     * Returns false if the lock is held by a different transaction.
     */
    public boolean acquireLock(int accountId, String txId) {
        Objects.requireNonNull(txId, "txId must not be null");

        String existing = locks.putIfAbsent(accountId, txId);
        if (existing == null) {
            transactionsWithLocks.add(txId);
            return true;
        }
        if (!existing.equals(txId)) {
            logger.warn("Lock on accountId={} is already held by txId={}; cannot acquire for txId={}",
                    accountId, existing, txId);
        }
        return existing.equals(txId);
    }

    /**
     * Release the lock on accountId if it is owned by txId.
     * Returns true if the lock was removed, false otherwise.
     */
    public boolean releaseLock(int accountId, String txId) {
        Objects.requireNonNull(txId, "txId must not be null");
        if (!locks.containsKey(accountId)) {
            logger.info("No lock exists on accountId={} to release for txId={}", accountId, txId);
            return true;
        }
        boolean removed = locks.remove(accountId, txId);
        if (removed) {
            // Check if this txId still holds any other locks
            if (!locks.containsValue(txId)) {
                transactionsWithLocks.remove(txId);
            }
        }
        if (!removed) logger.warn("Failed to release lock on accountId={} for txId={}: not held by this transaction", accountId, txId);
        return removed;
    }

    /**
     * Overload: acquire locks based on the Operation and this shard's ExecutionMode.
     * For TRANSFER:
     *   BOTH    -> lock sender and receiver
     *   SENDER  -> lock sender only
     *   RECEIVER-> lock receiver only
     * For BALANCEREQUEST:
     *   lock the requested account.
     *
     * Returns true only if all required locks are acquired; otherwise rolls back and returns false.
     */
    public boolean acquireLock(Operation operation, ExecutionMode mode, String txId) {
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(mode, "mode must not be null");
        Objects.requireNonNull(txId, "txId must not be null");

        int[] accountIds = OperationHelper.resolveAccountIds(operation, mode);
        if (accountIds.length == 0) {
            // Nothing to lock (defensive; currently not expected)
            return true;
        }

        // Deterministic order to reduce deadlock risk if multiple accounts are involved
        Arrays.sort(accountIds);

        List<Integer> acquired = new ArrayList<>(accountIds.length);
        for (int accountId : accountIds) {
            if (acquireLock(accountId, txId)) {
                acquired.add(accountId);
            } else {
                // Failed on one account: release any already-acquired locks for this tx
                for (int acquiredId : acquired) {
                    releaseLock(acquiredId, txId);
                }
                return false;
            }
        }
        return true;
    }

    /**
     * Acquire all required locks for the given operation and mode, then (for TRANSFER
     * in BOTH or SENDER mode) verify that the sender has sufficient balance.
     *
     * Returns:
     *  - false if locks could not be acquired, or if balance is insufficient;
     *    in both cases any locks acquired for this operation are released.
     *  - true if locks were acquired and (when applicable) balance is sufficient.
     */
    public boolean acquireLockAndCheckBalance(Operation operation,
                                              ExecutionMode mode,
                                              String txId,
                                              KeyValueStore<Double> database) {
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(mode, "mode must not be null");
        Objects.requireNonNull(txId, "txId must not be null");
        Objects.requireNonNull(database, "database must not be null");

        // Step 1: try to acquire all relevant locks (sender/receiver depending on mode)
        if (!acquireLock(operation, mode, txId)) {
            return false;
        }

        if (mode == ExecutionMode.BOTH) return true; // No balance check needed for intra-shard transfers

        // Step 2: only the shard responsible for debiting the sender checks funds
        if (operation.getOpCase() == Operation.OpCase.TRANSFER &&
                mode == ExecutionMode.SENDER) {

            int senderId = operation.getTransfer().getSender();
            double amount = operation.getTransfer().getAmount();

            Double currentBalance = database.get(senderId);

            boolean sufficient = currentBalance != null && currentBalance >= amount;

            if (!sufficient) {
                // This transaction cannot proceed; release locks acquired above.
                releaseLock(operation, mode, txId);
                return false;
            }
        }

        // No funds check needed (e.g. RECEIVER-only transfer or balance request),
        // or the check passed.
        logger.info("Locks acquired and balance check passed for txId={}", txId);
        return true;
    }

    /**
     * Overload: release locks based on the Operation and this shard's ExecutionMode.
     * Best-effort: releases any matching (accountId, txId) pairs.
     */
    public void releaseLock(Operation operation, ExecutionMode mode, String txId) {
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(mode, "mode must not be null");
        Objects.requireNonNull(txId, "txId must not be null");

        int[] accountIds = OperationHelper.resolveAccountIds(operation, mode);
        for (int accountId : accountIds) {
            releaseLock(accountId, txId);
        }
    }

    /**
     * Best-effort release of all locks held by txId.
     */
    public void releaseAllForTransaction(String txId) {
        Objects.requireNonNull(txId, "txId must not be null");
        locks.entrySet().removeIf(e -> txId.equals(e.getValue()));
        transactionsWithLocks.remove(txId);
    }

    /**
     * Check if any locks are currently held.
     */
    public boolean hasAnyLocks() {
        if (!locks.isEmpty()) {
            logger.info("Current locks held: {}", locks.values().toArray());
        }
        return !locks.isEmpty();
    }

    public String getRandomTransactionWithLocks() {
        Iterator<String> iterator = transactionsWithLocks.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    /**
     * Check if any locks are held by the given transaction.
     */
    public boolean hasLocks(String txId) {
        Objects.requireNonNull(txId, "txId must not be null");
        return transactionsWithLocks.contains(txId);
    }

    /**
     * Release all currently held locks on this node.
     * Intended to be called when the node transitions to a backup role
     * and should not own any application-level locks.
     */
    public void releaseAllLocks() {
        locks.clear();
        transactionsWithLocks.clear();
    }
}
