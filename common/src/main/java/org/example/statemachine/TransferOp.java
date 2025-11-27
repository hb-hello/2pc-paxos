package org.example.statemachine;

/**
 * Concrete Transfer operation: corresponds to the Transfer message in the proto.
 */
public record TransferOp(int sender, int receiver, double amount) implements StateMachineOperation {}

