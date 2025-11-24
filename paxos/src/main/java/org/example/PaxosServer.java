package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.messaging.PaxosService;

public class PaxosServer {
    private static final Logger logger = LogManager.getLogger(PaxosServer.class);

    private final int serverId;
    private final PaxosService paxosService;

    public PaxosServer(int serverId) {
        this.serverId = serverId;
        this.paxosService = new PaxosService(this);
        logger.info("PaxosServer {} initialized.", serverId);
    }

    public int getServerId() {
        return serverId;
    }

    public PaxosService getPaxosService() {
        return paxosService;
    }
}