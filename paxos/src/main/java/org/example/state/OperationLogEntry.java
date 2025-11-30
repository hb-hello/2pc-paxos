package org.example.state;

import org.example.ClientRequest;
import org.example.Phase;
import org.example.messaging.ServerMessage;

public record OperationLogEntry(ServerMessage<ClientRequest> request, Ballot ballot, OperationStatus status, Phase phase) {
    @Override
    public String toString() {
        return "OperationLogEntry{" +
                "request=" + request +
                ", ballot=" + ballot +
                ", status=" + status.name() +
                ", phase=" + phase.name() +
                '}';
    }
}
