package org.example.tpc;

import io.grpc.BindableService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.config.Config;
import org.example.consensus.TPCHooks;
import org.example.messaging.CLIServiceServer;
import org.example.messaging.ClientService;
import org.example.messaging.ServerMessage;
import org.example.messaging.TPCMessageSender;
import org.example.persistence.KeyValueStore;
import org.example.tpc.handlers.ClientRequestHandler;
import org.example.tpc.handlers.PrepareHandler;
import org.example.tpc.handlers.PreparedHandler;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

public class TPCServer implements TPCHooks {
    private static final Logger logger = LogManager.getLogger(TPCServer.class);

    private final int serverId;
    private final ConcurrentHashMap<Integer, Integer> leaderMap;
    private Map<Integer, Integer> accountIdToClusterMap;

    private final ExecutorManager executorManager;

    private final LockManager lockManager;
    private final KeyValueStore<Double> database;
    private final StateMachineOperator operator;
    private final ClientRequestTracker clientRequestTracker;
    private final TPCTimer tpcTimer;

    private final CLIServiceServer cliServiceServer;
    private final ClientService clientService;
    private final TPCMessageSender messageSender;
    private final MessageRetryManager retryManager;

    private final PaxosServer paxosServer;
    private final ClientRequestHandler clientRequestHandler;
    private final PrepareHandler prepareHandler;
    private final PreparedHandler preparedHandler;

    private final Set<NewViewMessage> newViews;

    public TPCServer(int serverId, CLIServiceServer cliServiceServer, ExecutorManager executorManager, KeyValueStore<Double> database) {
        this.serverId = serverId;
        this.leaderMap = new ConcurrentHashMap<>();

        for (int i = 0; i < Config.getServerClusterCount(); i++) {
            leaderMap.put(i, Config.getServerIdsInCluster(i).get(0)); // Initialize with first server as leader
        }

        this.executorManager = executorManager;
        this.lockManager = new LockManager();
        this.database = database;
        this.accountIdToClusterMap = database.getAllClusterIds();
        this.clientRequestTracker = new ClientRequestTracker();
        this.tpcTimer = new TPCTimer(Config.getServerTimeoutMillis() / 2L, executorManager.getTimerExecutor(), this::tpcTimerCallback);

        this.paxosServer = new PaxosServer(serverId, executorManager, this);
        this.cliServiceServer = cliServiceServer;
        this.clientService = new ClientService(this);

        this.operator = new StateMachineOperator(
                serverId,
                executorManager.getStateMachineExecutor(),
                database,
                paxosServer.getOperationLog(),
                clientRequestTracker,
                this::releaseLocksAndSendReply,
                paxosServer::getLatestCheckpointedSeqNum,
                paxosServer::addAndSendCheckpoint,
                lockManager::hasLocks
        );

        // this will perform warmup
        this.messageSender = new TPCMessageSender(serverId, executorManager.getNetworkExecutor());
        this.retryManager = new MessageRetryManager(messageSender, Config.getServerTimeoutMillis() / 2L, executorManager.getRetryExecutor(), clientRequestTracker::markAckReceived);

        this.clientRequestHandler = new ClientRequestHandler(
                serverId,
                lockManager,
                accountIdToClusterMap,
                database,
                operator,
                clientRequestTracker,
                paxosServer,
                messageSender,
                this::sendPrepare,
                this::markAbortedAndSend
        );
        this.prepareHandler = new PrepareHandler(clientRequestTracker, this::sendPrepared, this::sendAbort, this::handleClientRequest);
        this.preparedHandler = new PreparedHandler(tpcTimer, clientRequestTracker, this::triggerPaxos);

        this.newViews = ConcurrentHashMap.newKeySet();

        logger.info("TPCServer {} initialized.", serverId);
    }

    public List<BindableService> getServices() {
        return List.of(paxosServer.getPaxosService(), cliServiceServer, clientService);
    }

    public PaxosServer getPaxosServer() {
        return paxosServer;
    }

    public String getTrackedRequests() {
        return clientRequestTracker.printTrackedRequests();
    }

    public void warmup() {
        messageSender.warmup();
    }

    public Set<Integer> getModifiedAccounts() {
        logger.info("Getting modified accounts from StateMachineOperator, count so far: {}", operator.getModifiedAccounts().size());
        return operator.getModifiedAccounts();
    }

    public void setActive(boolean active) {
        paxosServer.setActive(active);
        messageSender.setActive(active);
    }

    public void setLeaderId(int leaderId) {
        int clusterIndex = Config.getServerClusterIndex(leaderId);
        leaderMap.put(clusterIndex, leaderId);
    }

    public void handleClientRequest(ServerMessage<ClientRequest> request) {
        executorManager.submitMessageProcessing(() -> clientRequestHandler.handle(request));
    }

    public void handlePrepare(ServerMessage<TPCPrepareMessage> message, StreamObserver<TPCAckMessage> responseObserver) {
        leaderMap.put(Config.getServerClusterIndex(message.payload().getSenderId()), message.payload().getSenderId());
        if (!paxosServer.isLeader()) paxosServer.initiateLeaderElection();
        prepareHandler.handle(message, responseObserver);
        if (messageSender.isActive()) {
            responseObserver.onNext(TPCAckMessage.newBuilder().build());
            responseObserver.onCompleted();
        }
    }

    public void handlePrepared(ServerMessage<TPCPreparedMessage> message) {
        leaderMap.put(Config.getServerClusterIndex(message.payload().getSenderId()), message.payload().getSenderId());
        executorManager.submitMessageProcessing(() -> preparedHandler.handle(message));
    }

    public void handleCommit(ServerMessage<TPCCommitMessage> message, StreamObserver<TPCAckMessage> responseObserver) {
        leaderMap.put(Config.getServerClusterIndex(message.payload().getSenderId()), message.payload().getSenderId());
        String requestId = message.payload().getRequestId();
        if(clientRequestTracker.isCommitted(requestId)) {
            // idempotent handling
            logger.info("Received duplicate TPC commit message for already committed requestId {}", requestId);
            TPCAckMessage ackMessage = TPCAckMessage.newBuilder().build();
            responseObserver.onNext(ackMessage);
            responseObserver.onCompleted();
            return;
        }
        if(clientRequestTracker.isConsensusCompletedPhase1(requestId)) triggerPaxos(clientRequestTracker.getRequest(requestId), Phase.COMMIT);
        else logger.error("No request with consensus complete found for requestId {} while handling TPC commit message", requestId);
        clientRequestTracker.setAckResponseObserver(requestId, responseObserver);
    }

    public void handleAbort(ServerMessage<TPCAbortMessage> message, StreamObserver<TPCAckMessage> responseObserver) {
        leaderMap.put(Config.getServerClusterIndex(message.payload().getSenderId()), message.payload().getSenderId());
        String requestId = message.payload().getRequestId();
        boolean isReceiver = clientRequestTracker.getExecutionMode(requestId) == ExecutionMode.RECEIVER;
        if(isReceiver && clientRequestTracker.isAborted(requestId)) {
            // idempotent handling
            logger.info("Received duplicate TPC abort message for already committed requestId {}", requestId);
            TPCAckMessage ackMessage = TPCAckMessage.newBuilder().build();
            responseObserver.onNext(ackMessage);
            responseObserver.onCompleted();
            return;
        }
        triggerPaxos(clientRequestTracker.getRequest(requestId), Phase.ABORT);
        if (isReceiver) clientRequestTracker.setAckResponseObserver(requestId, responseObserver);
        else {
            clientRequestTracker.markAckReceived(requestId);
            tpcTimer.stop(requestId);
        }
    }

    public void handleLeaderElected(NewLeader message) {
        int newLeaderId = message.getSenderId();
        int clusterIndex = Config.getServerClusterIndex(newLeaderId);
        leaderMap.put(clusterIndex, newLeaderId);
        logger.info("Updated leader for cluster {} to server {}", clusterIndex, newLeaderId);
        executorManager.submitMessageProcessing(() -> coordinateRequestsForOtherCluster(clusterIndex));
    }

    private void tpcTimerCallback(ServerMessage<ClientRequest> request) {
        logger.warn("TPC Server {} detected liveness timeout. Taking appropriate action.", serverId);
        if (clientRequestTracker.markAborted(request)) {
            logger.info("TPC timeout: Triggering Paxos for aborting request {}", request.getMessageId());
            triggerPaxos(request, Phase.ABORT);
        }
    }

    private void processPendingClientRequestsAsLeader() {
        for (ServerMessage<ClientRequest> request : clientRequestTracker.getPendingClientRequests()) {
            if (clientRequestTracker.getExecutionMode(request) == ExecutionMode.RECEIVER) continue;
            logger.info("Re-handling pending client request {} as leader", request.getMessageId());
            clientRequestHandler.handleClientRequestAsLeader(request);
        }
    }

    private void processPendingClientRequestsAsBackup() {
        for (ServerMessage<ClientRequest> request : clientRequestTracker.getPendingClientRequests()) {
            if (clientRequestTracker.getExecutionMode(request) == ExecutionMode.RECEIVER) continue;
            logger.info("Re-handling pending client request {} as backup", request.getMessageId());
            paxosServer.handleClientRequestAsNonLeader(request);
        }
    }

    private void releaseLocks(String requestId) {
        ExecutionMode mode = clientRequestTracker.getExecutionMode(requestId);
        lockManager.releaseLock(clientRequestTracker.getOperation(requestId), mode, requestId);
        paxosServer.refreshTimerOnExecute(requestId, lockManager.hasAnyLocks(), lockManager.getTransactionsWithLocks(), this::processPendingClientRequestsAsLeader);
        operator.markCommitted(requestId);
    }

    private void releaseLocksAndSendReply(String requestId, Phase phase) {
        if (phase != Phase.PREPARE) {
            releaseLocks(requestId);
            try {
                if (paxosServer.isLeader() && clientRequestTracker.getExecutionMode(requestId) != ExecutionMode.RECEIVER) {
                    ServerMessage<ClientReply> reply;
                    if (phase == Phase.ABORT) {
                        reply = new ServerMessage<>(ClientReply.newBuilder()
                                .setRequestId(requestId)
                                .setSenderId(serverId)
                                .setAborted(true)
                                .build());
                    } else reply = clientRequestTracker.getReply(requestId);
                    if (reply != null) {
                        messageSender.sendClientReply(reply);
                    } else {
                        logger.error("No reply found for committed request {}", requestId);
                    }
                }
            } catch (Exception e) {
                logger.error("Error while sending client reply for request {}: {}", requestId, e.getMessage());
            }
        }
    }

    // only done by coordinator i.e., in send mode
    public void sendPrepare(ServerMessage<ClientRequest> request) {
        try {
            int otherClusterIndex = clientRequestTracker.getOtherClusterIndex(request);

            if (otherClusterIndex != -1) {
                int otherClusterLeaderId = leaderMap.get(otherClusterIndex);
                TPCPrepareMessage tpcPrepareMessage = TPCPrepareMessage.newBuilder()
                        .setClientRequest(request.payload())
                        .setSenderId(serverId)
                        .build();
                sendPrepareWithAckHandler(otherClusterLeaderId, tpcPrepareMessage);
            } else
                logger.error("No other cluster index found while sending prepare for request {}", request.getMessageId());
        } catch (Exception e) {
            logger.error("Error while sending prepare for request {}: {}", request.getMessageId(), e.getMessage());
        }
    }

    private void sendPrepareWithAckHandler(int otherClusterLeaderId, TPCPrepareMessage tpcPrepareMessage) {
        StreamObserver<TPCAckMessage> ackObserver = new StreamObserver<>() {
            @Override
            public void onNext(TPCAckMessage value) {
                tpcTimer.start(new ServerMessage<>(tpcPrepareMessage.getClientRequest()));
            }

            @Override
            public void onError(Throwable t) {
                if (t instanceof StatusRuntimeException) {
                    StatusRuntimeException sre = (StatusRuntimeException) t;
                    if (sre.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                        logger.error("DEADLINE_EXCEEDED error: {}", sre.getMessage());
                        messageSender.broadcastPrepareToCluster(otherClusterLeaderId, new ServerMessage<>(tpcPrepareMessage));
                        tpcTimer.start(new ServerMessage<>(tpcPrepareMessage.getClientRequest()));
                    } else {
                        logger.error("gRPC error: {} - {}", sre.getStatus().getCode(), sre.getMessage());
                    }
                } else logger.error("Error while waiting for TPC Prepare ack: {}", t.getMessage());
            }

            @Override
            public void onCompleted() {
                // No action needed on completed
            }
        };
        messageSender.sendPrepare(otherClusterLeaderId, new ServerMessage<>(tpcPrepareMessage), ackObserver);
    }

    // only done by participant i.e., in receive mode
    public void markPreparedAndSend(String requestId) {
        if (clientRequestTracker.markPrepared(requestId)) {
            if (paxosServer.isLeader()) sendPrepared(requestId);
        }
    }

    private void sendPrepared(String requestId) {
        int otherClusterIndex = clientRequestTracker.getOtherClusterIndex(requestId);
        if (otherClusterIndex != -1) {
            int otherClusterLeaderId = leaderMap.get(otherClusterIndex);
            TPCPreparedMessage tpcPreparedMessage = TPCPreparedMessage.newBuilder()
                    .setRequestId(requestId)
                    .setSenderId(serverId)
                    .build();
            messageSender.sendPrepared(otherClusterLeaderId, new ServerMessage<>(tpcPreparedMessage));
        }
    }

    private void markCommittedAndSend(ServerMessage<ClientRequest> request) {
        markCommittedAndSend(request.getMessageId());
    }

    // only done by coordinator i.e., in send mode
    private void markCommittedAndSend(String requestId) {
        executorManager.submitMessageProcessing(() -> releaseLocksAndSendReply(requestId, Phase.COMMIT));
        clientRequestTracker.markCommitted(requestId);
        if (clientRequestTracker.isAckReceived(requestId)) logger.info("Ack already received for request {}, not sending commit again", requestId);
        else logger.info("Sending TPC commit for request {}", requestId);
        if (!clientRequestTracker.isAckReceived(requestId)) sendCommit(requestId);
    }

    private void markCommittedWithoutSending(String requestId) {
        releaseLocks(requestId);
        clientRequestTracker.markCommitted(requestId);
    }

    private void sendCommit(String requestId) {
        int otherClusterIndex = clientRequestTracker.getOtherClusterIndex(requestId);
        if (otherClusterIndex != -1) {
            int otherClusterLeaderId = leaderMap.get(otherClusterIndex);
            TPCCommitMessage tpcCommitMessage = TPCCommitMessage.newBuilder()
                    .setRequestId(requestId)
                    .setSenderId(serverId)
                    .build();
            retryManager.startCommitRetries(otherClusterLeaderId, new ServerMessage<>(tpcCommitMessage));
        } else logger.error("No other cluster index found while sending commit for request {}", requestId);
    }

    // can be done by coordinator only i.e., send mode
    private void markAbortedAndSend(ServerMessage<ClientRequest> request) {
        releaseLocksAndSendReply(request.getMessageId(), Phase.ABORT);
        clientRequestTracker.markAborted(request);
        if (!clientRequestTracker.isAckReceived(request)) sendAbort(request);
    }

    // can be done by participant only i.e., receive mode
    private void markAbortedWithoutSending(String requestId) {
        releaseLocks(requestId);
        clientRequestTracker.markAborted(requestId);
    }

    private void sendAbort(ServerMessage<ClientRequest> request) {
        int otherClusterIndex = clientRequestTracker.getOtherClusterIndex(request);
        ExecutionMode mode = clientRequestTracker.getExecutionMode(request);
        if (otherClusterIndex != -1) {
            int otherClusterLeaderId = leaderMap.get(otherClusterIndex);
            TPCAbortMessage tpcAbortMessage = TPCAbortMessage.newBuilder()
                    .setRequestId(request.getMessageId())
                    .setSenderId(serverId)
                    .build();
            if (mode == ExecutionMode.SENDER)
                retryManager.startAbortRetries(otherClusterLeaderId, new ServerMessage<>(tpcAbortMessage));
            else messageSender.sendAbortWithoutResponse(otherClusterLeaderId, new ServerMessage<>(tpcAbortMessage));
        } else logger.error("No other cluster index found while sending abort for request {}",
                request.getMessageId());
    }

    private void coordinateRequestsForOtherCluster(int otherClusterIndex) {
        if (!paxosServer.isLeader()) return;
        Set<String> requests = clientRequestTracker.getRequestsForOtherCluster(otherClusterIndex);
        for (String requestId : requests) {
            if (!clientRequestTracker.isAccepted(requestId)) continue;
            ServerMessage<ClientRequest> request = clientRequestTracker.getRequest(requestId);
            if (!clientRequestTracker.isPrepared(requestId)) {
                sendPrepare(request);
                continue;
            }
            if (clientRequestTracker.isAckReceived(requestId)) continue;
            if (clientRequestTracker.isCommitted(requestId)) {
                sendCommit(requestId);
                continue;
            }
            if (clientRequestTracker.isAborted(requestId)) sendAbort(request);
        }
    }

    // ---------- TPCHooks implementation ----------

    public void onPaxosCommit(ServerMessage<CommitMessage> commitMessage) {
        long seqNum = commitMessage.payload().getSequenceNumber();
        Phase phase = commitMessage.payload().getPhase();
        ServerMessage<ClientRequest> request =
                new ServerMessage<>(commitMessage.payload().getRequest());
        ExecutionMode mode = clientRequestTracker.getExecutionMode(request);

        switch (phase) {
            case PREPARE, INTRA_SHARD:
                handlePrepareOrIntraShardPhase(seqNum, phase, request, mode);
                break;

            case COMMIT:
                handleCommitPhase(request, mode);
                break;

            case ABORT:
                handleAbortPhase(request, mode);
                break;

            default: break;
        }
    }

    private void handlePrepareOrIntraShardPhase(long seqNum,
                                                Phase phase,
                                                ServerMessage<ClientRequest> request,
                                                ExecutionMode mode) {
        // Common work for PREPARE and INTRA_SHARD
        clientRequestTracker.markConsensusCompletedPhase1(request);
        operator.execute(seqNum, mode);

        // Only PREPARE has extra behavior based on mode
        if (phase != Phase.PREPARE) {
            return;
        }

        switch (mode) {
            case SENDER:
                if (clientRequestTracker.isPrepared(request)) triggerPaxos(request, Phase.COMMIT);
                break;

            case RECEIVER:
                markPreparedAndSend(request.getMessageId());
                break;

            default: break;
        }
    }

    private void handleCommitPhase(ServerMessage<ClientRequest> request,
                                   ExecutionMode mode) {
        clientRequestTracker.markConsensusCompletedPhase2(request);
        if (mode == ExecutionMode.SENDER && paxosServer.isLeader()) {
            markCommittedAndSend(request);
        } else {
            markCommittedWithoutSending(request.getMessageId());
            if (mode == ExecutionMode.RECEIVER && paxosServer.isLeader()) clientRequestTracker.sendAckResponse(request.getMessageId());
        }
    }

    private void handleAbortPhase(ServerMessage<ClientRequest> request,
                                  ExecutionMode mode) {
        clientRequestTracker.markConsensusCompletedPhase2(request);
        operator.undo(request.getMessageId());
        if (mode == ExecutionMode.SENDER && paxosServer.isLeader()) {
            markAbortedAndSend(request);
        } else {
            markAbortedWithoutSending(request.getMessageId());
            if (mode == ExecutionMode.RECEIVER && paxosServer.isLeader()) clientRequestTracker.sendAckResponse(request.getMessageId());
        }
    }

    private void triggerPaxos(String requestId, Phase phase) {
        ServerMessage<ClientRequest> request = clientRequestTracker.getRequest(requestId);
        if (request != null) {
            triggerPaxos(request, phase);
        } else {
            logger.error("No request found for requestId {} while triggering Paxos for phase {}", requestId, phase);
        }
    }

    private void triggerPaxos(ServerMessage<ClientRequest> request, Phase phase) {
        executorManager.submitMessageProcessing(() ->
                paxosServer.triggerAccept(request, phase)
        );
    }

    @Override
    public void onRoleChangeToLeader() {
        processPendingClientRequestsAsLeader();
    }

    @Override
    public void onNewClientRequest(ServerMessage<ClientRequest> request) {
        ExecutionMode executionMode = OperationHelper.resolveExecutionMode(serverId, request.payload().getOperation(), accountIdToClusterMap);
        int otherClusterIndex = OperationHelper.resolveOtherClusterIndex(serverId, request.payload().getOperation(), accountIdToClusterMap);
        logger.info("Client request {} with execution mode {} and other cluster index {} being added / updated in tracker. (either during new view or receiving accept/commit from leader)", request.getMessageId(), executionMode, otherClusterIndex);
        clientRequestTracker.addAcceptedRequest(request, executionMode, otherClusterIndex);
    }

    @Override
    public void onPaxosNewView(ServerMessage<NewViewMessage> newViewMessage) {
        newViews.add(newViewMessage.payload());
        messageSender.broadcastLeaderElected(serverId);

        NewViewMessage newView = newViewMessage.payload();
        for (AcceptMessage acceptMessage : newView.getAcceptLogList()) {
            Phase phase = acceptMessage.getPhase();
            ServerMessage<ClientRequest> request = new ServerMessage<>(acceptMessage.getRequest());
            onNewClientRequest(request);
            switch (phase) {
                case PREPARE -> sendPrepare(request);
                case COMMIT, ABORT, INTRA_SHARD -> {
                    //nothing to do here
                }
                default -> logger.error("Unknown phase {} in accept message during new view handling", phase);
            }
        }

        for (CommitMessage commitMessage : newView.getCommitLogList()) {
            if (clientRequestTracker.isAckReceived(new ServerMessage<>(commitMessage.getRequest()))) continue;
            Phase phase = commitMessage.getPhase();
            ServerMessage<ClientRequest> request = new ServerMessage<>(commitMessage.getRequest());
            onNewClientRequest(request);
            switch (phase) {
                case PREPARE -> {
                    if (!clientRequestTracker.isPrepared(request)) sendPrepare(request);
                    onPaxosCommit(new ServerMessage<>(commitMessage));
                }
                case COMMIT, ABORT, INTRA_SHARD -> onPaxosCommit(new ServerMessage<>(commitMessage));
                default -> logger.error("Unknown phase {} in commit message during new view handling", phase);
            }
        }
    }

    @Override
    public void onRoleChangeToBackup() {
        tpcTimer.shutdown();
        retryManager.shutdown();
        lockManager.releaseAllLocks();
        processPendingClientRequestsAsBackup();
    }

    @Override
    public void applyCheckpoint(long seqNum, String snapshot) {
        operator.applyCheckpoint(seqNum, snapshot);
    }

    public void reset() {
        paxosServer.reset();
        lockManager.releaseAllLocks();
        tpcTimer.shutdown();
        retryManager.shutdown();
        for (int i = 0; i < Config.getServerClusterCount(); i++) {
            leaderMap.put(i, Config.getServerIdsInCluster(i).get(0)); // Initialize with first server as leader
        }
        this.accountIdToClusterMap = database.getAllClusterIds();
        clientRequestTracker.reset();
    }

    public Set<NewViewMessage> getNewViews() {
        return Set.copyOf(newViews);
    }
}
