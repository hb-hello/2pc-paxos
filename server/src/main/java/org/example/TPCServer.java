package org.example;

import io.grpc.BindableService;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.messaging.CLIServiceServer;
import org.example.messaging.ClientService;
import org.example.messaging.ServerMessage;
import org.example.messaging.TPCMessageSender;
import org.example.persistence.KeyValueStore;
import org.example.statemachine.BankStateMachine;

import java.util.List;
import java.util.Set;

public class TPCServer {
    private static final Logger logger = LogManager.getLogger(TPCServer.class);

    private final int serverId;

    private final BankStateMachine stateMachine;

    private final ClientRequestTracker clientRequestTracker;

    private final CLIServiceServer cliServiceServer;
    private final ClientService clientService;
    private final TPCMessageSender messageSender;

    private final PaxosServer paxosServer;

    public TPCServer(int serverId, CLIServiceServer cliServiceServer, ExecutorManager executorManager, KeyValueStore<Double> database) {
        this.serverId = serverId;

        this.stateMachine = new BankStateMachine(database);

        this.clientRequestTracker = new ClientRequestTracker();

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

    public void handleClientRequest(ServerMessage<ClientRequest> request, StreamObserver<ClientReply> responseObserver) {

        if (clientRequestTracker.hasRequest(request)) {
            logger.info("Received duplicate client request {}. Checking for stored reply.", request);
            ServerMessage<ClientReply> replyMessage = clientRequestTracker.getReply(request);
            if (replyMessage != null) {
                logger.info("Found stored reply for duplicate client request {}. Resending reply.", request);
                responseObserver.onNext(replyMessage.payload());
                responseObserver.onCompleted();
            }
            logger.info("No stored reply found for duplicate client request {}. Ignoring request.", request);
        } else {
            clientRequestTracker.addRequest(request);
            paxosServer.handleClientRequest(request);
        }

        ClientRequest clientRequest = request.payload();

        try {
            Operation operation = clientRequest.getOperation();
            OperationResult result = stateMachine.execute(operation, ExecutionMode.BOTH);

            ClientReply reply = ClientReply.newBuilder()
                    .setResult(result)
                    .setSenderId(serverId)
                    .setClientId(clientRequest.getClientId())
                    .setTimestamp(clientRequest.getTimestamp())
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
}
