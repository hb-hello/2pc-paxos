package org.example.state;

public enum OperationStatus {
    PREPARED,
    COMMITTED,
    EXECUTED,
    CHECKPOINTED,
    NONE
}