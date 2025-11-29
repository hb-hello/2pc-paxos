package org.example.consensus.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.ClientRequest;
import org.example.messaging.ServerMessage;
import org.example.state.PaxosState;

public class ClientRequestHandler {
    private static final Logger logger = LogManager.getLogger(ClientRequestHandler.class);

    private final PaxosState state;

    public ClientRequestHandler(PaxosState state) {
        this.state = state;
    }

    public void handle(ServerMessage<ClientRequest> request) {
        logger.info("Handling client request: {}", request.getMessageId());
    }
}
