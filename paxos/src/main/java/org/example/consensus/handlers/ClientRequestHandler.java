package org.example.consensus.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.ClientRequest;
import org.example.messaging.PaxosMessageSender;
import org.example.messaging.ServerMessage;
import org.example.state.PaxosState;
import org.example.state.Role;

public class ClientRequestHandler {
    private static final Logger logger = LogManager.getLogger(ClientRequestHandler.class);

    private final PaxosState state;
    private final PaxosMessageSender messageSender;

    public ClientRequestHandler(PaxosState state, PaxosMessageSender messageSender) {
        this.state = state;
        this.messageSender = messageSender;
    }

    public void handle(ServerMessage<ClientRequest> request, Runnable leaderInitiationCallback) {
        logger.info("Handling client request: {}", request.getMessageId());

        state.runSync(() -> {
            if (state.getRole() == Role.BACKUP) {
                messageSender.forwardClientRequest(state.getLeaderId(), request);
            } else if (state.getRole() == Role.CANDIDATE) {
                if (!state.hasSentPrepare()) {
                    leaderInitiationCallback.run();
                }
            } else if (state.getRole() == Role.LEADER) {
                // Logic to send accept to all backups would go here
            }
        });

        // if role is backup, forward client request to leader
        // if role is candidate, check if we have a promise that hasn't expired and attempt to trigger leader election
        // if role is leader, send accept to all backups
    }
}
