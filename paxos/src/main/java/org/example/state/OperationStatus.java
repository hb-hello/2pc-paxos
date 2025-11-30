package org.example.state;

public enum OperationStatus {
    ACCEPTED,
    COMMITTED,
    EXECUTED,
    CHECKPOINTED,
    NONE
}