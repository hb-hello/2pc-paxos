package org.example.tpc;

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.config.Config;
import org.example.consensus.LivenessTimer;
import org.example.messaging.CLIServiceServer;
import org.example.messaging.ClientService;
import org.example.messaging.ServerMessage;
import org.example.messaging.TPCMessageSender;
import org.example.persistence.KeyValueStore;
import org.example.state.OperationLog;
import org.example.statemachine.BankStateMachine;
import org.example.tpc.handlers.ClientRequestHandler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class TPCServer {
    private static final Logger logger = LogManager.getLogger(TPCServer.class);

    private final int serverId;

    private final ExecutorManager executorManager;

    private final LockManager lockManager;
    private final KeyValueStore<Double> database;
    private final StateMachineOperator operator;
    private final ClientRequestTracker clientRequestTracker;
    private final LivenessTimer tpcTimer;

    private final CLIServiceServer cliServiceServer;
    private final ClientService clientService;
    private final TPCMessageSender messageSender;

    private final PaxosServer paxosServer;
    private final ClientRequestHandler clientRequestHandler;

    public TPCServer(int serverId, CLIServiceServer cliServiceServer, ExecutorManager executorManager, KeyValueStore<Double> database) {
        this.serverId = serverId;

        this.executorManager = executorManager;
        this.lockManager = new LockManager();
        this.database = database;
        this.clientRequestTracker = new ClientRequestTracker();
        this.tpcTimer = new LivenessTimer(Config.getServerTimeoutMillis() * 3L, this::tpcTimerCallback);

        this.paxosServer = new PaxosServer(serverId, executorManager);
        this.cliServiceServer = cliServiceServer;
        this.clientService = new ClientService(this);

        this.operator = new StateMachineOperator(executorManager.getStateMachineExecutor(),
                database,
                paxosServer.getOperationLog());

        // this will perform warmup
        this.messageSender = new TPCMessageSender(serverId, executorManager.getNetworkExecutor());
        this.clientRequestHandler = new ClientRequestHandler(
                serverId,
                lockManager,
                database,
                operator,
                clientRequestTracker,
                paxosServer,
                tpcTimer
        );
    }

    public List<BindableService> getServices() {
        return List.of(paxosServer.getPaxosService(), cliServiceServer, clientService);
    }

    public void warmup() {
        messageSender.warmup();
    }

    public Set<Integer> getModifiedAccounts() {
        return operator.getModifiedAccounts();
    }

    public void setActive(boolean active) {
        paxosServer.setActive(active);
        messageSender.setActive(active);
    }

    public void handleClientRequest(ServerMessage<ClientRequest> request, StreamObserver<ClientReply> responseObserver) {
        executorManager.submitMessageProcessing(() -> clientRequestHandler.handle(request, responseObserver));
    }

    private void tpcTimerCallback() {
        logger.warn("TPC Server {} detected liveness timeout. Taking appropriate action.", serverId);
        // Implement appropriate action on timeout, e.g., notify Paxos server or reset state
    }
}
