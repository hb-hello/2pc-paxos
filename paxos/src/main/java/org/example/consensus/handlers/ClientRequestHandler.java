package org.example.consensus.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.ClientRequest;
import org.example.ExecutorManager;
import org.example.messaging.PaxosMessageSender;
import org.example.messaging.ServerMessage;
import org.example.state.PaxosState;
import org.example.state.Role;

public class ClientRequestHandler {
    private static final Logger logger = LogManager.getLogger(ClientRequestHandler.class);

    private final PaxosState state;
    private final PaxosMessageSender messageSender;
    private final ExecutorManager executorManager;

    public ClientRequestHandler(PaxosState state, PaxosMessageSender messageSender, ExecutorManager executorManager) {
        this.state = state;
        this.messageSender = messageSender;
        this.executorManager = executorManager;
    }

    public void handle(ServerMessage<ClientRequest> request, Runnable leaderInitiationCallback) {
        logger.info("Handling client request: {}", request.getMessageId());

        state.runSync(() -> {
            if (state.isBackup()) {
                executorManager.submitMessageProcessing(() -> messageSender.forwardClientRequest(state.getLeaderId(), request));
            } else if (state.isCandidate()) {
                if (!state.hasSentPrepare()) {
                    leaderInitiationCallback.run();
                }
            }
        });
    }
}
