package org.example.statemachine;

/**
 * Concrete Transfer operation: corresponds to the Transfer message in the proto.
 */
public record TransferOp(String sender, String receiver, double amount) implements StateMachineOperation {}

