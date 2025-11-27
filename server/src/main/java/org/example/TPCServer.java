package org.example;

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.messaging.CLIServiceServer;
import org.example.messaging.ClientService;
import org.example.messaging.TPCMessageSender;
import org.example.persistence.KeyValueStore;
import org.example.statemachine.BankStateMachine;

import java.util.List;
import java.util.Set;

public class TPCServer {
    private static final Logger logger = LogManager.getLogger(TPCServer.class);

    private final int serverId;

    private final KeyValueStore<Double> database;
    private final BankStateMachine stateMachine;

    private final CLIServiceServer cliServiceServer;
    private final ClientService clientService;
    private final TPCMessageSender messageSender;

    private final PaxosServer paxosServer;

    public TPCServer(int serverId, CLIServiceServer cliServiceServer, ExecutorManager executorManager, KeyValueStore<Double> database) {
        this.serverId = serverId;

        this.database = database;
        this.stateMachine = new BankStateMachine(database);

        this.paxosServer = new PaxosServer(serverId, executorManager);
        this.cliServiceServer = cliServiceServer;
        this.clientService = new ClientService(this);

        // this will perform warmup
        this.messageSender = new TPCMessageSender(serverId, executorManager.getNetworkExecutor());
    }

    public List<BindableService> getServices() {
        return List.of(paxosServer.getPaxosService(), cliServiceServer, clientService);
    }

    public void warmup() {
        messageSender.warmup();
    }

    public Set<Integer> getModifiedAccounts() {
        return stateMachine.getModifiedAccounts();
    }

    public void setActive(boolean active) {
        paxosServer.setActive(active);
        messageSender.setActive(active);
    }

    public void handleClientRequest(ClientRequest request, StreamObserver<ClientReply> responseObserver) {
        try {
            Operation operation = request.getOperation();
            OperationResult result = stateMachine.execute(operation, ExecutionMode.BOTH);

            ClientReply reply = ClientReply.newBuilder()
                    .setResult(result)
                    .setSenderId(serverId)
                    .setClientId(request.getClientId())
                    .setTimestamp(request.getTimestamp())
                    .build();

            logger.info("Server {} executed client request {} with result {} : {}",
                    serverId, request, result.getResultCase(), result.getResultCase() == OperationResult.ResultCase.BALANCE
                            ? result.getBalance()
                            : result.getSuccess()
            );

            responseObserver.onNext(reply);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error executing client request {}: {}", request, e.getMessage());
        }
    }

    public enum ExecutionMode {
        BOTH,
        SENDER,
        RECEIVER
    }
}
