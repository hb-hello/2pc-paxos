package org.example.tpc.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.consensus.LivenessTimer;
import org.example.messaging.ServerMessage;
import org.example.messaging.TPCMessageSender;
import org.example.persistence.KeyValueStore;
import org.example.tpc.*;

public class ClientRequestHandler {
    private static final Logger logger = LogManager.getLogger(ClientRequestHandler.class);

    private final int serverId;
    private final LockManager lockManager;
    private final KeyValueStore<Double> database;
    private final StateMachineOperator operator;
    private final ClientRequestTracker clientRequestTracker;
    private final PaxosServer paxosServer;
    private final LivenessTimer tpcTimer;
    private final TPCMessageSender messageSender;

    public ClientRequestHandler(int serverId,
                                LockManager lockManager,
                                KeyValueStore<Double> database,
                                StateMachineOperator operator,
                                ClientRequestTracker clientRequestTracker,
                                PaxosServer paxosServer,
                                LivenessTimer tpcTimer,
                                TPCMessageSender messageSender) {
        this.serverId = serverId;
        this.lockManager = lockManager;
        this.database = database;
        this.operator = operator;
        this.clientRequestTracker = clientRequestTracker;
        this.paxosServer = paxosServer;
        this.tpcTimer = tpcTimer;
        this.messageSender = messageSender;
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

        ClientRequest clientRequest = request.payload();

        if (!clientRequest.getIsReadOnly()) {
            ExecutionMode executionMode = OperationHelper.resolveExecutionMode(serverId, clientRequest.getOperation(), database);
            int otherClusterIndex = OperationHelper.resolveOtherClusterIndex(serverId, clientRequest.getOperation(), database);
            if (lockManager.acquireLockAndCheckBalance(clientRequest.getOperation(), executionMode, request.getMessageId(), database)) {

                // do this regardless of role
                clientRequestTracker.addRequest(request, executionMode, otherClusterIndex);
                paxosServer.handleClientRequest(request);

                if (paxosServer.isLeader()) {
                    if (executionMode == ExecutionMode.BOTH) {
                        paxosServer.triggerAccept(request, Phase.INTRA_SHARD);
                        clientRequestTracker.markAccepted(request);
                    } else {
                        // send tpc prepare
                        paxosServer.triggerAccept(request, Phase.PREPARE);
                        clientRequestTracker.markAccepted(request);
                    }
                }

            } else {
                logger.info("Failed to acquire locks or insufficient balance for request {}", request);
                return;
            }
        }

        if (clientRequest.getIsReadOnly()) executeReadOnlyAndReply(request);
    }

    private void executeReadOnlyAndReply(ServerMessage<ClientRequest> request) {
        try {
            ClientRequest clientRequest = request.payload();
            Operation resultOperation = clientRequest.getOperation();
            ExecutionMode executionMode = OperationHelper.resolveExecutionMode(serverId, resultOperation, database);

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
