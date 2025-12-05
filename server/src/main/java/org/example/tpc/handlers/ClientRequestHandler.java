package org.example.tpc.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.messaging.ServerMessage;
import org.example.messaging.TPCMessageSender;
import org.example.persistence.KeyValueStore;
import org.example.tpc.*;

import java.util.Map;
import java.util.function.Consumer;

public class ClientRequestHandler {
    private static final Logger logger = LogManager.getLogger(ClientRequestHandler.class);

    private final int serverId;
    private final LockManager lockManager;
    private final Map<Integer, Integer> accountIdToClusterMap;
    private final KeyValueStore<Double> database;
    private final StateMachineOperator operator;
    private final ClientRequestTracker clientRequestTracker;
    private final PaxosServer paxosServer;
    private final TPCMessageSender messageSender;
    private final Consumer<ServerMessage<ClientRequest>> sendPrepare;

    public ClientRequestHandler(int serverId,
                                LockManager lockManager,
                                Map<Integer, Integer> accountIdToClusterMap,
                                KeyValueStore<Double> database,
                                StateMachineOperator operator,
                                ClientRequestTracker clientRequestTracker,
                                PaxosServer paxosServer,
                                TPCMessageSender messageSender,
                                Consumer<ServerMessage<ClientRequest>> sendPrepare,
                                TPCTimer tpcTimer) {
        this.serverId = serverId;
        this.lockManager = lockManager;
        this.accountIdToClusterMap = accountIdToClusterMap;
        this.database = database;
        this.operator = operator;
        this.clientRequestTracker = clientRequestTracker;
        this.paxosServer = paxosServer;
        this.messageSender = messageSender;
        this.sendPrepare = sendPrepare;
    }

    public void handle(ServerMessage<ClientRequest> request) {

        if (clientRequestTracker.hasRequest(request)) {
            logger.info("Received duplicate client request {}. Checking for stored reply.", request.getMessageId());
            ServerMessage<ClientReply> replyMessage = clientRequestTracker.getReply(request);
            if (replyMessage != null) {
                logger.info("Found stored reply for duplicate client request {}. Resending reply.", request.getMessageId());
                messageSender.sendClientReply(replyMessage);
                return;
            }
            logger.info("No stored reply found for duplicate client request {}. Ignoring request.", request.getMessageId());
            return;
        }

        if (!request.payload().getIsReadOnly()) {
            if (paxosServer.isLeader()) handleClientRequestAsLeader(request);
            else {
                ExecutionMode executionMode = OperationHelper.resolveExecutionMode(serverId, request.payload().getOperation(), accountIdToClusterMap);
                int otherClusterIndex = OperationHelper.resolveOtherClusterIndex(serverId, request.payload().getOperation(), accountIdToClusterMap);
                clientRequestTracker.addRequest(request, executionMode, otherClusterIndex);
                logger.info("Added client request {} to tracker as non-leader with execution mode {} and otherClusterIndex {}", request.getMessageId(), executionMode.name(), otherClusterIndex);
                paxosServer.handleClientRequestAsNonLeader(request);
            }

        } else executeReadOnlyAndReply(request);
    }

    public void handleClientRequestAsLeader(ServerMessage<ClientRequest> request) {
        ClientRequest clientRequest = request.payload();
        ExecutionMode executionMode = OperationHelper.resolveExecutionMode(serverId, clientRequest.getOperation(), accountIdToClusterMap);

        if (!clientRequestTracker.hasRequest(request)) {
            int otherClusterIndex = OperationHelper.resolveOtherClusterIndex(serverId, request.payload().getOperation(), accountIdToClusterMap);
            clientRequestTracker.addRequest(request, executionMode, otherClusterIndex);
            logger.info("Added client request {} to tracker as leader with execution mode {} and otherClusterIndex {}", request.getMessageId(), executionMode.name(), otherClusterIndex);
        }

        if (lockManager.acquireLockAndCheckBalance(clientRequest.getOperation(), executionMode, request.getMessageId(), database)) {

            if (executionMode == ExecutionMode.BOTH) {
                //Intra-Shard
                paxosServer.triggerAccept(request, Phase.INTRA_SHARD);
                clientRequestTracker.markAccepted(request);
                clientRequestTracker.markIntraShard(request);
            } else {
                //Cross-Shard
                sendPrepare.accept(request);
                paxosServer.triggerAccept(request, Phase.PREPARE);
                clientRequestTracker.markAccepted(request);
            }
        } else {
            logger.info("Failed to acquire locks or insufficient balance for request {}", request);
//            if (!clientRequestTracker.isAccepted(request)) clientRequestTracker.removeRequest(request);
        }
    }

    private void executeReadOnlyAndReply(ServerMessage<ClientRequest> request) {
        try {
            ClientRequest clientRequest = request.payload();
            Operation resultOperation = clientRequest.getOperation();
            ExecutionMode executionMode = OperationHelper.resolveExecutionMode(serverId, resultOperation, accountIdToClusterMap);

            OperationResult result = operator.executeReadOnly(resultOperation);

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

            messageSender.sendClientReply(new ServerMessage<>(reply));

        } catch (Exception e) {
            logger.error("Error executing client request {}: {}", request, e.getMessage());
        }
    }
}
