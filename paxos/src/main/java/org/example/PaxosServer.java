package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.messaging.PaxosService;
import org.example.persistence.KeyValueStore;

public class PaxosServer {
    private static final Logger logger = LogManager.getLogger(PaxosServer.class);

    private final int serverId;
    private final PaxosService paxosService;

    private final KeyValueStore<Double> database;

    public PaxosServer(int serverId, KeyValueStore<Double> database) {
        this.serverId = serverId;
        this.paxosService = new PaxosService(this);
        this.database = database;
        logger.info("PaxosServer {} initialized.", serverId);
    }

    public int getServerId() {
        return serverId;
    }

    public PaxosService getPaxosService() {
        return paxosService;
    }
}