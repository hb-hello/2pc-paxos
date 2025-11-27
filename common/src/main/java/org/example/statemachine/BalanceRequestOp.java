package org.example.statemachine;

/**
 * Concrete BalanceRequest operation: corresponds to the BalanceRequest message in the proto.
 */
public record BalanceRequestOp(int accountId) implements StateMachineOperation { }

