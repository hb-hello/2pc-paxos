package org.example.state;

import org.example.ClientRequest;

public record OperationLogEntry(ClientRequest request, Ballot ballot, OperationStatus status) {
    @Override
    public String toString() {
        return "OperationLogEntry{" +
                "request=" + request +
                ", ballot=" + ballot +
                ", status=" + status +
                '}';
    }
}
