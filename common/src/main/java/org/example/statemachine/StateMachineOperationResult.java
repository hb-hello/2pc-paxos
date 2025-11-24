package org.example.statemachine;

/**
 * Simpler alternative to the sealed OperationResult hierarchy: a single record
 * with exactly one of 'success' (boolean) or 'balance' (double) present.
 */
public record StateMachineOperationResult(Boolean success, Double balance) {
    public StateMachineOperationResult {
        // Exactly one must be non-null
        if ((success == null) == (balance == null)) {
            throw new IllegalArgumentException("Exactly one of 'success' or 'balance' must be set");
        }
    }

    public static StateMachineOperationResult success(boolean value) {
        return new StateMachineOperationResult(value, null);
    }

    public static StateMachineOperationResult balance(double value) {
        return new StateMachineOperationResult(null, value);
    }

    public boolean isSuccess() { return success != null; }
    public boolean isBalance() { return balance != null; }
}

