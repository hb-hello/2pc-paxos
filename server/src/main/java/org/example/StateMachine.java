package org.example;

/**
 * State machine execution contract used by ServerState.
 */
public interface StateMachine {
    /**
     * Execute a single deterministic operation and return a proto OperationResult.
     */
    OperationResult execute(Operation operation, ExecutionMode mode);

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
