package org.example.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.benchmark.ClientMetricsListener;
import org.example.config.Config;
import org.example.messaging.CLIMessageSender;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientNode {

    private static final Logger logger = LogManager.getLogger(ClientNode.class);

    // Max total attempts: 1 leader try + (MAX_BROADCAST_ROUNDS * full-cluster broadcasts)
    private static final int MAX_BROADCAST_ROUNDS = 3;

    private final int clientId;
    private final CLIMessageSender messageSender;
    private final Map<Integer, Integer> accountIdToClusterIndex;
    private final Map<Integer, Integer> clusterIndexLeaderId;
    private final Map<Integer, int[]> clusterIndexNodeIds;
    private final Map<PendingRequestKey, PendingRequest> pendingRequests = new ConcurrentHashMap<>();

    private volatile ClientMetricsListener metricsListener;

    public void setMetricsListener(ClientMetricsListener listener) {
        this.metricsListener = listener;
    }

    public ClientNode(int clientId,
                      CLIMessageSender messageSender,
                      Map<Integer, Integer> accountIdToClusterIndex) {
        this.clientId = clientId;
        this.messageSender = messageSender;
        this.accountIdToClusterIndex = accountIdToClusterIndex;
        this.clusterIndexLeaderId = new HashMap<>();
        this.clusterIndexNodeIds = new HashMap<>();

        int clusterCount = Config.getServerClusterCount();
        int clusterSize = Config.getServerClusterSize(); // 3 in the base config[attached_file:2]

        for (int clusterIdx = 0; clusterIdx < clusterCount; clusterIdx++) {
            int leaderId = clusterIdx * clusterSize + 1;
            clusterIndexLeaderId.put(clusterIdx, leaderId);

            int[] nodeIds = new int[clusterSize];
            for (int j = 0; j < clusterSize; j++) {
                nodeIds[j] = clusterIdx * clusterSize + 1 + j;
            }
            clusterIndexNodeIds.put(clusterIdx, nodeIds);
        }
    }

    public void processTransaction(String raw) {
        String line = raw == null ? "" : raw.trim();
        if (line.isEmpty()) {
            return;
        }

        // 1) F / R commands
        char firstLetter = TransactionParser.firstLetter(line);
        if (firstLetter == 'F' || firstLetter == 'f'
                || firstLetter == 'R' || firstLetter == 'r') {

            boolean isFail = Character.toUpperCase(firstLetter) == 'F';
            int nodeIdInt = TransactionParser.parseNodeId(line);
            NodeId nodeId = NodeId.newBuilder()
                    .setNodeId(nodeIdInt)
                    .build();

            if (isFail) {
                handleFail(nodeId);
            } else {
                handleRecover(nodeId);
            }
            return;
        }

        // 2) Normalize numeric content
        String content = TransactionParser.stripOuterParens(line);

        // 3) Transfer vs balance → always build Operation and then ClientRequest
        if (content.contains(",")) {
            String[] parts = TransactionParser.splitAndTrim(content);
            if (parts.length == 3) {
                // Transfer: sender, receiver, amount
                int sender = Integer.parseInt(parts[0]);
                int receiver = Integer.parseInt(parts[1]);
                double amount = Double.parseDouble(parts[2]);

                Transfer transfer = Transfer.newBuilder()
                        .setSender(sender)
                        .setReceiver(receiver)
                        .setAmount(amount)
                        .build(); // adjusted proto with int fields in your code

                Operation op = Operation.newBuilder()
                        .setTransfer(transfer)
                        .build();

                buildAndSendClientRequest(op, sender);
                return;
            } else if (parts.length == 1) {
                // Single field with comma noise: treat as balance request
                int accountId = Integer.parseInt(parts[0]);

                BalanceRequest balanceRequest = BalanceRequest.newBuilder()
                        .setAccountId(accountId)
                        .build();

                Operation op = Operation.newBuilder()
                        .setBalanceRequest(balanceRequest)
                        .build();

                buildAndSendClientRequest(op, accountId);
                return;
            } else {
                logger.warn("Unrecognized transaction format: {}", raw);
                return;
            }
        } else {
            // Single value -> BalanceRequest, e.g. "7800" or "(7800)"
            int accountId = Integer.parseInt(content.trim());

            BalanceRequest balanceRequest = BalanceRequest.newBuilder()
                    .setAccountId(accountId)
                    .build();

            Operation op = Operation.newBuilder()
                    .setBalanceRequest(balanceRequest)
                    .build();

            buildAndSendClientRequest(op, accountId);
        }
    }

    /**
     * Build the final ClientRequest from an Operation (transfer or balance) and start async send.
     */
    private void buildAndSendClientRequest(Operation operation, int accountId) {
        boolean readOnly = operation.getOpCase() == Operation.OpCase.BALANCE_REQUEST;
        ClientRequest request = ClientRequest.newBuilder()
                .setTimestamp(System.currentTimeMillis()) // client-side timestamp[attached_file:1]
                .setClientId(accountId)
                .setOperation(operation)
                .setIsReadOnly(readOnly)
                .build();

        sendClientRequestWithRetry(request);
    }

    /**
     * Core retry logic:
     * - First attempt goes to the cluster leader.
     * - On timeout/transport error, broadcast to all nodes in that cluster.
     * - If a full broadcast round fails, repeat up to MAX_BROADCAST_ROUNDS.
     * All of this is based on ListenableFuture callbacks; no sleeping or blocking.
     */
    private void sendClientRequestWithRetry(ClientRequest request) {
        int targetClusterIndex;
        if (request.getOperation().hasTransfer()) {
            int accountId = request.getOperation().getTransfer().getSender();
            targetClusterIndex = accountIdToClusterIndex.get(accountId);
        } else if (request.getOperation().hasBalanceRequest()) {
            int accountId = request.getOperation().getBalanceRequest().getAccountId();
            targetClusterIndex = accountIdToClusterIndex.get(accountId);
        } else {
            logger.warn("Unknown operation type in client request.");
            return;
        }

        PendingRequest ctx = new PendingRequest(request, targetClusterIndex);
        pendingRequests.put(PendingRequestKey.of(ctx.request), ctx);
        sendToLeader(ctx);
    }

    private void sendToLeader(PendingRequest ctx) {
        if (ctx.completed) {
            return;
        }
        int leaderNodeId = clusterIndexLeaderId.get(ctx.clusterIndex);
        long deadlineMillis = Config.getClientTimeoutMillis();

        logger.debug("Sending request {} to leader {} of cluster {}",
                ctx.request.getTimestamp(), leaderNodeId, ctx.clusterIndex);

        messageSender.sendClientRequestWithDeadline(leaderNodeId, ctx.request, deadlineMillis);
    }

    private void handleReplyFromNode(PendingRequest ctx,
                                     int nodeId,
                                     ClientReply reply,
                                     boolean fromBroadcast) {
        long latency = 0L;
        ClientRequest completedRequest = null;
        synchronized (ctx) {
            if (ctx.completed) {
                return;
            }
            ctx.completed = true;
            latency = System.currentTimeMillis() - ctx.startTimeMillis;
            completedRequest = ctx.request;
            logger.info("Request {} completed via node {} in {} ms, result={}",
                    ctx.request.getTimestamp(), nodeId, latency, reply.getResult());
        }

        ClientMetricsListener listener = metricsListener;
        if (listener != null) {
            listener.onRequestCompleted(completedRequest, latency);
        }
        pendingRequests.remove(PendingRequestKey.of(ctx.request));
    }

    public void handleClientReply(ClientReply reply) {
        PendingRequest ctx = pendingRequests.get(PendingRequestKey.of(reply));
        if (ctx == null) {
            logger.warn("No pending request found for reply timestamp {} client {}", reply.getTimestamp(), reply.getClientId());
            return;
        }
        handleReplyFromNode(ctx, reply.getSenderId(), reply, false);
    }

    private void startNextBroadcastRoundLocked(PendingRequest ctx) {
        if (ctx.completed) {
            return;
        }
        if (ctx.broadcastRound >= MAX_BROADCAST_ROUNDS) {
            logger.warn("Request {} failed after {} broadcast rounds.",
                    ctx.request.getTimestamp(), ctx.broadcastRound);
            ctx.completed = true;
            return;
        }

        ctx.broadcastRound++;
        int[] nodeIds = clusterIndexNodeIds.get(ctx.clusterIndex);
        ctx.pendingBroadcastResponses = nodeIds.length;

        long deadlineMillis = Config.getClientTimeoutMillis();

        logger.debug("Broadcast round {} for request {} to cluster {}",
                ctx.broadcastRound, ctx.request.getTimestamp(), ctx.clusterIndex);

        // Important: we can send RPCs outside the synchronized block to avoid
        // holding the lock while scheduling network calls.
        // Copy state we need first:
        ClientRequest request = ctx.request.toBuilder().setIsReadOnly(false).build();

        // Release lock before actually sending
        // (we know ctx.broadcastRound and pendingBroadcastResponses are set)
        // NOTE: we must NOT touch ctx fields after this point without re-acquiring the lock.
        // So we capture what we need into locals first.

        // Send RPCs
        for (int nodeId : nodeIds) {
            messageSender.sendClientRequestWithDeadline(nodeId, request, deadlineMillis);
        }
    }



    private void handleFail(NodeId nodeId) {
        // TODO: send F request (fail node) via CLI/control service
        try {
            Thread.sleep(50);
            messageSender.failNode(nodeId);
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleRecover(NodeId nodeId) {
        // TODO: send R request (recover node) via CLI/control service
        try {
            Thread.sleep(50);
            messageSender.recoverNode(nodeId);
            Thread.sleep(50);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Per-client-request state for retry/broadcast logic.
     * Lives only as long as its futures and callbacks are alive.
     */
    private static final class PendingRequest {
        final ClientRequest request;
        final int clusterIndex;
        final long startTimeMillis;

        boolean completed = false;
        int broadcastRound = 0;
        int pendingBroadcastResponses = 0;

        PendingRequest(ClientRequest request, int clusterIndex) {
            this.request = request;
            this.clusterIndex = clusterIndex;
            this.startTimeMillis = System.currentTimeMillis();
        }
    }

    private record PendingRequestKey(long timestamp, int clientId) {
        static PendingRequestKey of(ClientRequest request) {
            return new PendingRequestKey(request.getTimestamp(), request.getClientId());
        }

        static PendingRequestKey of(ClientReply reply) {
            return new PendingRequestKey(reply.getTimestamp(), reply.getClientId());
        }
    }
}
