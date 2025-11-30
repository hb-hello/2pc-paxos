package org.example.tpc;

import io.grpc.BindableService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.config.Config;
import org.example.consensus.LivenessTimer;
import org.example.consensus.TPCHooks;
import org.example.messaging.CLIServiceServer;
import org.example.messaging.ClientService;
import org.example.messaging.ServerMessage;
import org.example.messaging.TPCMessageSender;
import org.example.persistence.KeyValueStore;
import org.example.tpc.handlers.ClientRequestHandler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class TPCServer implements TPCHooks {
    private static final Logger logger = LogManager.getLogger(TPCServer.class);

    private final int serverId;
    private final ConcurrentHashMap<Integer, Integer> leaderMap;

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
        this.leaderMap = new ConcurrentHashMap<>();

        for (int i = 0; i < Config.getServerClusterCount(); i++) {
            leaderMap.put(i, Config.getServerIdsInCluster(i).get(0)); // Initialize with first server as leader
        }

        this.executorManager = executorManager;
        this.lockManager = new LockManager();
        this.database = database;
        this.clientRequestTracker = new ClientRequestTracker();
        this.tpcTimer = new LivenessTimer(Config.getServerTimeoutMillis() * 3L, this::tpcTimerCallback);

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
        this.clientRequestHandler = new ClientRequestHandler(
                serverId,
                lockManager,
                database,
                operator,
                clientRequestTracker,
                paxosServer,
                tpcTimer,
                messageSender
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

    public void setLeaderId(int leaderId) {
        int clusterIndex = Config.getServerClusterIndex(leaderId);
        leaderMap.put(clusterIndex, leaderId);
    }

    public void handleClientRequest(ServerMessage<ClientRequest> request) {
        executorManager.submitMessageProcessing(() -> clientRequestHandler.handle(request));
    }

    private void tpcTimerCallback() {
        logger.warn("TPC Server {} detected liveness timeout. Taking appropriate action.", serverId);
        // Implement appropriate action on timeout, e.g., notify Paxos server or reset state
    }

    // ---------- TPCHooks implementation ----------

    @Override
    public void onPaxosCommit(ServerMessage<CommitMessage> commitMessage) {
        long seqNum = commitMessage.payload().getSequenceNumber();
        Phase phase = commitMessage.payload().getPhase();
        ServerMessage<ClientRequest> request = new ServerMessage<>(commitMessage.payload().getRequest());
        ExecutionMode mode = clientRequestTracker.getExecutionMode(request);

        if (phase == Phase.PREPARE || phase == Phase.INTRA_SHARD) {
            operator.execute(seqNum, mode);
            if (phase == Phase.PREPARE && clientRequestTracker.isPrepared(request))
                markCommittedAndSend(request);
        } else releaseLocksAndSendReply(request, phase);
    }

    private void releaseLocksAndSendReply(ServerMessage<ClientRequest> request, Phase phase) {
        if (phase == Phase.INTRA_SHARD || (phase == Phase.COMMIT && clientRequestTracker.isCommitted(request))) {
            ExecutionMode mode = clientRequestTracker.getExecutionMode(request);
            lockManager.releaseLock(request.payload().getOperation(), mode, request.getMessageId());
            ServerMessage<ClientReply> replyMessage = clientRequestTracker.getReply(request);
            if (replyMessage != null) {
                messageSender.sendClientReply(replyMessage);
            } else {
                logger.error("No reply found for committed request {}", request.getMessageId());
            }
        }
    }

    private void markCommittedAndSend(ServerMessage<ClientRequest> request) {
        int otherClusterIndex = clientRequestTracker.getOtherClusterIndex(request);

        if (otherClusterIndex != -1) {
            int otherClusterLeaderId = leaderMap.get(otherClusterIndex);
            TPCCommitMessage tpcCommitMessage = TPCCommitMessage.newBuilder()
                    .setRequestId(request.getMessageId())
                    .setSenderId(serverId)
                    .build();
            messageSender.sendCommit(otherClusterLeaderId, new ServerMessage<>(tpcCommitMessage));
        } else logger.error("No other cluster index found for request {}", request.getMessageId());

        clientRequestTracker.markCommitted(request);
    }
}
