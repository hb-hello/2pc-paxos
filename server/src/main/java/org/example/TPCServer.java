package org.example;

import io.grpc.BindableService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.messaging.CLIServiceServer;
import org.example.messaging.TPCMessageSender;
import org.example.persistence.DatabaseManager;
import org.example.persistence.KeyValueStore;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TPCServer {
    private static final Logger logger = LogManager.getLogger(TPCServer.class);

    private final int serverId;

    private final KeyValueStore<Double> database;
    private final Set<Integer> modifiedAccounts;

    private final CLIServiceServer cliServiceServer;
    private final TPCMessageSender messageSender;

    private final PaxosServer paxosServer;

    public TPCServer(int serverId, CLIServiceServer cliServiceServer, ExecutorManager executorManager, KeyValueStore<Double> database) {
        this.serverId = serverId;

        this.database = database;
        this.modifiedAccounts = new HashSet<>();

        this.paxosServer = new PaxosServer(serverId, database);
        this.cliServiceServer = cliServiceServer;

        // this will perform warmup
        this.messageSender = new TPCMessageSender(serverId, executorManager.getNetworkExecutor());
    }

    public List<BindableService> getServices() {
        return List.of(paxosServer.getPaxosService(), cliServiceServer);
    }

    public void warmup() {
        messageSender.warmup();
    }

    public Set<Integer> getModifiedAccounts() {
        return modifiedAccounts;
    }
}
