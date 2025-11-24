package org.example;

import io.grpc.BindableService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.messaging.CLIServiceServer;
import org.example.messaging.TPCMessageSender;

import java.util.List;

public class TPCServer {
    private static final Logger logger = LogManager.getLogger(TPCServer.class);

    private final int serverId;
    private final CLIServiceServer cliServiceServer;
    private final PaxosServer paxosServer;

    private final TPCMessageSender messageSender;


    public TPCServer(int serverId, CLIServiceServer cliServiceServer, ExecutorManager executorManager) {
        this.serverId = serverId;
        this.paxosServer = new PaxosServer(serverId);
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
}
