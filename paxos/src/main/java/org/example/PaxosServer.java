package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.consensus.handlers.ClientRequestHandler;
import org.example.messaging.PaxosMessageSender;
import org.example.messaging.PaxosService;
import org.example.messaging.ServerMessage;
import org.example.state.PaxosState;

public class PaxosServer {
    private static final Logger logger = LogManager.getLogger(PaxosServer.class);

    private final ExecutorManager executorManager;

    private final PaxosState state;
    private final PaxosService paxosService;
    private final PaxosMessageSender messageSender;

    private final ClientRequestHandler clientRequestHandler;

    public PaxosServer(int serverId, ExecutorManager executorManager) {
        this.executorManager = executorManager;
        this.state = new PaxosState(serverId, executorManager.getStateExecutor());
        this.paxosService = new PaxosService(this);
        this.messageSender = new PaxosMessageSender(serverId, executorManager.getNetworkExecutor());

        this.clientRequestHandler = new ClientRequestHandler(state);
        logger.info("PaxosServer {} initialized.", serverId);
    }

    public int getServerId() {
        return state.getServerId();
    }

    public PaxosService getPaxosService() {
        return paxosService;
    }

    public void setActive(boolean active) {
        messageSender.setActive(active);
    }

    public void handleClientRequest(ServerMessage<ClientRequest> request) {
        executorManager.submitMessageProcessing(() -> clientRequestHandler.handle(request));
    }
}