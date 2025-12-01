package org.example.tpc;

import io.grpc.BindableService;
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
import org.example.tpc.handlers.PreparedHandler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TPCServer implements TPCHooks {
    private static final Logger logger = LogManager.getLogger(TPCServer.class);

    private final int serverId;
    private final ConcurrentHashMap<Integer, Integer> leaderMap;

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
    private final PreparedHandler preparedHandler;

    public TPCServer(int serverId, CLIServiceServer cliServiceServer, ExecutorManager executorManager, KeyValueStore<Double> database) {
        this.serverId = serverId;
        this.leaderMap = new ConcurrentHashMap<>();

        for (int i = 0; i < Config.getServerClusterCount(); i++) {
            leaderMap.put(i, Config.getServerIdsInCluster(i).get(0)); // Initialize with first server as leader
        }

        this.executorManager = executorManager;
        this.lockManager = new LockManager();
        this.database = database;
        this.clientRequestTracker = new ClientRequestTracker();
        this.tpcTimer = new TPCTimer(Config.getServerTimeoutMillis() * 3L, executorManager.getTimerExecutor(), this::tpcTimerCallback);

        this.paxosServer = new PaxosServer(serverId, executorManager, this);
        this.cliServiceServer = cliServiceServer;
        this.clientService = new ClientService(this);

        this.operator = new StateMachineOperator(serverId,
                executorManager.getStateMachineExecutor(),
                database,
                paxosServer.getOperationLog(),
                clientRequestTracker,
                this::releaseLocksAndSendReply);

        // this will perform warmup
        this.messageSender = new TPCMessageSender(serverId, executorManager.getNetworkExecutor());
        this.retryManager = new MessageRetryManager(messageSender, Config.getServerTimeoutMillis() * 2L, executorManager.getRetryExecutor());

        this.clientRequestHandler = new ClientRequestHandler(
                serverId,
                lockManager,
                database,
                operator,
                clientRequestTracker,
                paxosServer,
                messageSender,
                this::sendPrepare,
                tpcTimer
        );
        this.preparedHandler = new PreparedHandler(tpcTimer, clientRequestTracker, this::markCommittedAndSend);
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

    public void setLeaderId(int leaderId) {
        int clusterIndex = Config.getServerClusterIndex(leaderId);
        leaderMap.put(clusterIndex, leaderId);
    }

    public void handleClientRequest(ServerMessage<ClientRequest> request) {
        executorManager.submitMessageProcessing(() -> clientRequestHandler.handle(request));
    }

    public void handlePrepared(ServerMessage<TPCPreparedMessage> message) {
        executorManager.submitMessageProcessing(() -> preparedHandler.handle(message));
    }

    private void tpcTimerCallback(ServerMessage<ClientRequest> request) {
        logger.warn("TPC Server {} detected liveness timeout. Taking appropriate action.", serverId);
        if (clientRequestTracker.markAborted(request)) {
            operator.undo(request.getMessageId());
            releaseLocks(request.getMessageId());
            markAbortedAndSend(request);
        }
    }

    private void releaseLocks(String requestId) {
        ExecutionMode mode = clientRequestTracker.getExecutionMode(requestId);
        lockManager.releaseLock(clientRequestTracker.getOperation(requestId), mode, requestId);
        operator.markCommittedOrAborted(requestId);
    }

    private void releaseLocksAndSendReply(ServerMessage<ClientRequest> request, Phase phase) {
        executorManager.submitMessageProcessing(() -> releaseLocksAndSendReply(request.getMessageId(), phase));
    }

    private void releaseLocksAndSendReply(String requestId, Phase phase) {
        if (phase == Phase.INTRA_SHARD || (phase == Phase.COMMIT && clientRequestTracker.isCommitted(requestId))) {
            releaseLocks(requestId);
            paxosServer.refreshTimerOnExecute();
            ServerMessage<ClientReply> replyMessage = clientRequestTracker.getReply(requestId);
            if (replyMessage != null) {
                messageSender.sendClientReply(replyMessage);
            } else {
                logger.error("No reply found for committed request {}", requestId);
            }
        }
    }

    public void sendPrepare(ServerMessage<ClientRequest> request) {
        int otherClusterIndex = clientRequestTracker.getOtherClusterIndex(request);

        if (otherClusterIndex != -1) {
            int otherClusterLeaderId = leaderMap.get(otherClusterIndex);
            TPCPrepareMessage tpcPrepareMessage = TPCPrepareMessage.newBuilder()
                    .setClientRequest(request.payload())
                    .setSenderId(serverId)
                    .build();
            messageSender.sendPrepare(otherClusterLeaderId, new ServerMessage<>(tpcPrepareMessage));
            tpcTimer.start(request);
        } else logger.error("No other cluster index found while sending prepare for request {}", request.getMessageId());
    }

    private void markCommittedAndSend(ServerMessage<ClientRequest> request) {
        markCommittedAndSend(request.getMessageId());
    }

    private void markCommittedAndSend(String requestId) {
        clientRequestTracker.markCommitted(requestId);
        releaseLocksAndSendReply(requestId, Phase.COMMIT);

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

    private void markAbortedAndSend(ServerMessage<ClientRequest> request) {
        int otherClusterIndex = clientRequestTracker.getOtherClusterIndex(request);

        if (otherClusterIndex != -1) {
            int otherClusterLeaderId = leaderMap.get(otherClusterIndex);
            TPCAbortMessage tpcAbortMessage = TPCAbortMessage.newBuilder()
                    .setRequestId(request.getMessageId())
                    .setSenderId(serverId)
                    .build();
            retryManager.startAbortRetries(otherClusterLeaderId, new ServerMessage<>(tpcAbortMessage));
        } else logger.error("No other cluster index found while sending abort for request {}", request.getMessageId());

        clientRequestTracker.markAborted(request);
    }

    // ---------- TPCHooks implementation ----------

    @Override
    public void onPaxosCommit(ServerMessage<CommitMessage> commitMessage) {
        long seqNum = commitMessage.payload().getSequenceNumber();
        Phase phase = commitMessage.payload().getPhase();
        ServerMessage<ClientRequest> request = new ServerMessage<>(commitMessage.payload().getRequest());
        ExecutionMode mode = clientRequestTracker.getExecutionMode(request);

        clientRequestTracker.markConsensusCompleted(request);

        if (phase == Phase.PREPARE || phase == Phase.INTRA_SHARD) {
            operator.execute(seqNum, mode);
            if (phase == Phase.PREPARE && clientRequestTracker.isPrepared(request)) {
                executorManager.submitMessageProcessing(() ->
                        paxosServer.triggerAccept(request, Phase.COMMIT, commitMessage.payload().getSequenceNumber())
                );
                markCommittedAndSend(request);
            }
        }
    }

    @Override
    public void onRoleChangeToLeader() {
        for (ServerMessage<ClientRequest> request : clientRequestTracker.getPendingClientRequests()) {
            logger.info("Re-handling pending client request {} as leader", request.getMessageId());
            clientRequestHandler.handleClientRequestAsLeader(request);
        }
    }

    @Override
    public void onNewClientRequest(ServerMessage<ClientRequest> request) {
        ExecutionMode executionMode = OperationHelper.resolveExecutionMode(serverId, request.payload().getOperation(), database);
        int otherClusterIndex = OperationHelper.resolveOtherClusterIndex(serverId, request.payload().getOperation(), database);
        logger.info("New client request {} with execution mode {} and other cluster index {} being added to tracker.", request.getMessageId(), executionMode, otherClusterIndex);
        clientRequestTracker.addRequest(request, executionMode, otherClusterIndex);
        clientRequestTracker.markAccepted(request);
    }

    @Override
    public void onPaxosNewView(ServerMessage<NewViewMessage> newViewMessage) {
        NewViewMessage newView = newViewMessage.payload();

        for (AcceptMessage acceptMessage: newView.getAcceptLogList()) {
            Phase phase = acceptMessage.getPhase();
            ServerMessage<ClientRequest> request = new ServerMessage<>(acceptMessage.getRequest());
            onNewClientRequest(request);
            switch (phase) {
                case PREPARE -> sendPrepare(request);
                case ABORT -> {
                    releaseLocks(request.getMessageId()); //shouldn't be needed but just in case
                    markAbortedAndSend(request);
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
                case COMMIT, INTRA_SHARD -> onPaxosCommit(new ServerMessage<>(commitMessage));
                case ABORT -> {
                    releaseLocks(request.getMessageId()); //shouldn't be needed but just in case
                    markAbortedAndSend(request);
                }
                default -> logger.error("Unknown phase {} in commit message during new view handling", phase);
            }
        }
    }

    @Override
    public void onRoleChangeToBackup() {
        tpcTimer.shutdown();
        retryManager.shutdown();
        lockManager.releaseAllLocks();
    }
}
