package org.example;

import org.example.tpc.ExecutionMode;

/**
 * State machine execution contract used by ServerState.
 */
public interface StateMachine {
    /**
     * Execute a single deterministic operation and return a proto OperationResult.
     */
    OperationResult execute(Operation operation, ExecutionMode mode);

    void recordWalEntry(String compositeKey, double beforeBalance);
    Double readWalEntry(String compositeKey);
    void deleteWalEntry(String compositeKey);

    /**
     * Consistent snapshot of application state for CLI/inspection.
     */
    String snapshot();

    boolean applySnapshot(String snapshot);

    /**
     * Clear all application state (used between test sets and on ServerState.reset()).
     */
    void reset();
}
