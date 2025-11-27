package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.messaging.PaxosMessageSender;
import org.example.messaging.PaxosService;

public class PaxosServer {
    private static final Logger logger = LogManager.getLogger(PaxosServer.class);

    private final int serverId;
    private final PaxosService paxosService;
    private final PaxosMessageSender messageSender;

    public PaxosServer(int serverId, ExecutorManager executorManager) {
        this.serverId = serverId;
        this.paxosService = new PaxosService(this);
        this.messageSender = new PaxosMessageSender(serverId, executorManager.getNetworkExecutor());
        logger.info("PaxosServer {} initialized.", serverId);
    }

    public int getServerId() {
        return serverId;
    }

    public PaxosService getPaxosService() {
        return paxosService;
    }

    public void setActive(boolean active) {
        messageSender.setActive(active);
    }
}