package org.example;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.config.Config;
import org.example.consensus.LivenessTimer;
import org.example.consensus.TPCHooks;
import org.example.consensus.handlers.*;
import org.example.messaging.PaxosMessageSender;
import org.example.messaging.PaxosService;
import org.example.messaging.ServerMessage;
import org.example.metrics.PaxosMetricsListener;
import org.example.state.OperationLog;
import org.example.state.OperationLogEntry;
import org.example.state.PaxosState;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class PaxosServer {
    private static final Logger logger = LogManager.getLogger(PaxosServer.class);

    private final ExecutorManager executorManager;

    private final LivenessTimer promiseTimer;
    private final LivenessTimer clientRequestTimer;

    private final PaxosState state;
    private final TPCHooks tpcHooks;
    private AtomicBoolean leaderElectionInProgress;

    private final PaxosService paxosService;
    private final PaxosMessageSender messageSender;

    private final ClientRequestHandler clientRequestHandler;
    private final PrepareHandler prepareHandler;
    private final PromiseHandler promiseHandler;
    private final NewViewHandler newViewHandler;
    private final AcceptHandler acceptHandler;
    private final AcceptedHandler acceptedHandler;
    private final CommitHandler commitHandler;

    private final PaxosMetricsListener metricsListener;

    public PaxosServer(int serverId, ExecutorManager executorManager, TPCHooks tpcHooks) {
        this.executorManager = executorManager;
        this.promiseTimer = new LivenessTimer(getRandom(Config.getServerTimeoutMillis() / 4), this::promiseTimerCallback);
        this.clientRequestTimer = new LivenessTimer(Config.getServerTimeoutMillis(), this::clientRequestTimerCallback);

        this.metricsListener = new PaxosMetricsListener();
        this.state = new PaxosState(serverId, executorManager.getStateExecutor(), this::onNewClientRequest, tpcHooks::onRoleChangeToBackup, metricsListener);
        this.leaderElectionInProgress = new AtomicBoolean(false);

        this.paxosService = new PaxosService(this);
        this.messageSender = new PaxosMessageSender(serverId, executorManager.getNetworkExecutor());

        this.clientRequestHandler = new ClientRequestHandler(state, messageSender, executorManager);
        this.prepareHandler = new PrepareHandler(state, promiseTimer);
        this.promiseHandler = new PromiseHandler(state, promiseTimer, this::triggerNewView);
        this.newViewHandler = new NewViewHandler(state, promiseTimer, this::acceptLatestCheckpointSeqSeen);
        this.acceptHandler = new AcceptHandler(state, clientRequestTimer, promiseTimer);
        this.acceptedHandler = new AcceptedHandler(state, this::triggerCommit);
        this.commitHandler = new CommitHandler(state, this::triggerCommit);

        this.tpcHooks = tpcHooks;

        logger.info("PaxosServer {} initialized.", serverId);
    }

    private long getRandom(long num) {
        Random random = new Random();
        long max = num + 20;
        long min = num - 20;
        return random.nextLong(max - min + 1) + min;
    }

    public int getServerId() {
        return state.getServerId();
    }

    public PaxosService getPaxosService() {
        return paxosService;
    }

    public void setActive(boolean active) {
        messageSender.setActive(active);
    }

    public boolean isLeader() {
        return state.isLeader();
    }

    public boolean isCandidate() {
        return state.isCandidate();
    }

    public boolean isBackup() {
        return state.isBackup();
    }

    public OperationLog getOperationLog() {
        return state.getOperationLog();
    }

    public String printOperationLog() {
        return state.printOperationLog();
    }

    public ServerMessage<ClientRequest> getClientRequestBySeqNum(long sequenceNumber) {
        return state.getOperationLog().getRequest(sequenceNumber);
    }

    public Phase getPhaseBySeqNum(long sequenceNumber) {
        OperationLogEntry entry = state.getLogEntry(sequenceNumber);
        if (entry != null) {
            return entry.phase();
        } else {
            return null;
        }
    }

    public void handleClientRequestAsNonLeader(ServerMessage<ClientRequest> request) {
        // already called from within message processing thread
        clientRequestHandler.handle(request, this::initiateLeaderElection);
        if (!state.isCandidate()) clientRequestTimer.startIfNotRunning("handling client request as non-leader : " + request.getMessageId());
    }

    public void handlePrepare(ServerMessage<PrepareMessage> prepareMessage, StreamObserver<PromiseMessage> responseObserver) {
        executorManager.submitMessageProcessing(() -> prepareHandler.handle(prepareMessage, responseObserver));
    }

    public void handleNewView(ServerMessage<NewViewMessage> newView, StreamObserver<AcceptedMessage> responseObserver) {
        executorManager.submitMessageProcessing(() -> newViewHandler.handle(newView, responseObserver));
    }

    public void handleAccept(ServerMessage<AcceptMessage> message, StreamObserver<AcceptedMessage> responseObserver) {
        executorManager.submitMessageProcessing(() -> acceptHandler.handle(message, responseObserver));
    }

    public void handleCommit(ServerMessage<CommitMessage> commitMessage) {
        executorManager.submitMessageProcessing(() -> commitHandler.handle(commitMessage));
    }

    public void handleCheckpoint(ServerMessage<CheckpointMessage> checkpointMessage) {
        if (state.getApplyingCheckpoint()) return;
        state.setApplyingCheckpoint(true);
        tpcHooks.applyCheckpoint(checkpointMessage.payload().getSequenceNumber(), checkpointMessage.payload().getState());
    }

    public void addAndSendCheckpoint(long sequenceNumber, String snapshot) {
        try {
            if (state.addCheckpoint(sequenceNumber, snapshot)) {
                if (isLeader()) messageSender.broadcastCheckpoint(state.getLatestCheckpointMessage());
            }
            state.setApplyingCheckpoint(false);
        } catch (Exception e) {
            logger.error("Error adding and sending checkpoint for sequence number {}: ", sequenceNumber, e);
        }
    }

    public long getLatestCheckpointedSeqNum() {
        return state.getLatestCheckpointedSeqNum();
    }

    public ServerMessage<CheckpointMessage> getCheckpointMessage(long seqNum) {
        return state.getCheckpointMessage(seqNum);
    }

    public void acceptLatestCheckpointSeqSeen() {
        executorManager.submitMessageProcessing(() -> {
            try {
                long seqNum = state.getLatestCheckpointSeqSeen();
                if (state.getLatestCheckpointedSeqNum() < seqNum) {
                    ServerMessage<CheckpointRequest> checkpointRequest = new ServerMessage<>(CheckpointRequest.newBuilder().
                            setSequenceNumber(seqNum)
                            .build());
                    StreamObserver<CheckpointMessage> responseObserver = new StreamObserver<CheckpointMessage>() {
                        @Override
                        public void onNext(CheckpointMessage checkpointMessage) {
                            ServerMessage<CheckpointMessage> message = new ServerMessage<>(checkpointMessage);
                            logger.info("Received checkpoint message in response to checkpoint request : {}", message);
                            handleCheckpoint(message);
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            logger.error("Error in receiving checkpoint message");
                        }

                        @Override
                        public void onCompleted() {}
                    };
                    messageSender.broadcastCheckpointRequest(checkpointRequest, responseObserver);
                }
            } catch (Exception e) {
                logger.error("Error processing latest checkpoint sequence number seen");
            }
        });
    }

    public void onNewClientRequest(ServerMessage<ClientRequest> request) {
        tpcHooks.onNewClientRequest(request);
    }

    public void initiateLeaderElection() {
        if (!leaderElectionInProgress.get() && !promiseTimer.isRunning()) {
            leaderElectionInProgress.set(true);
            clientRequestTimer.stop();
            state.runSync(() -> {
                state.transitionToCandidate();
                Ballot ballot = state.getBallot().toProto();
                PrepareMessage prepareMessage = PrepareMessage.newBuilder()
                        .setBallot(ballot)
                        .build();
                executorManager.submitMessageProcessing(() -> {
                    try {
                        messageSender.broadcastPrepare(new ServerMessage<>(prepareMessage), promiseHandler.handler());
                        promiseTimer.restart("initiating leader election for ballot: " + state.getBallot());
                    } catch (Exception e) {
                        logger.error("Error broadcasting prepare messages during leader election: ", e);
                    }
                });
                state.setSentPrepare(true);
            });
        }
    }

    public void triggerNewView(org.example.state.Ballot newBallot) {
        if (state.checkBallotAndTransitionToLeader(newBallot)) {
            acceptLatestCheckpointSeqSeen();
            promiseTimer.stop();
            ServerMessage<NewViewMessage> newView = new ServerMessage<>(state.getNewView());
            tpcHooks.onPaxosNewView(newView); // refactor to be directly called from the operationLog loop
            try {
                messageSender.broadcastNewView(newView, acceptedHandler.handler());
                executorManager.submitMessageProcessing(tpcHooks::onRoleChangeToLeader);
            } catch (Exception e) {
                logger.error("Error broadcasting new view messages during leader election: ", e);
            }
            leaderElectionInProgress.set(false);
        }
    }

    public void triggerAccept(ServerMessage<ClientRequest> request, Phase phase) {
        long seqNum = state.acceptRequest(request, phase);
        if (seqNum == -1L) return;
        promiseTimer.stop();
        startTimerAndBroadcastAccept(request, phase, seqNum);
    }

    private void startTimerAndBroadcastAccept(ServerMessage<ClientRequest> request, Phase phase, long seqNum) {
        clientRequestTimer.startIfNotRunning("handling client request to trigger accept for " + phase.name() + " phase : " + request.getMessageId());
        try {
            if (state.isLeader()) {
                AcceptMessage acceptMessage = AcceptMessage.newBuilder()
                        .setSequenceNumber(seqNum)
                        .setBallot(state.getBallot().toProto())
                        .setPhase(phase)
                        .setRequest(request.payload())
                        .build();
                messageSender.broadcastAccept(new ServerMessage<>(acceptMessage), acceptedHandler.handler());
            }
        } catch (Exception e) {
            logger.error("Error broadcasting accept messages during {} phase: ", phase.name(), e);
        }
    }

    public void triggerCommit(long sequenceNumber) {
        try {
            OperationLogEntry entry = state.getLogEntry(sequenceNumber);
            CommitMessage commit = CommitMessage.newBuilder()
                    .setSequenceNumber(sequenceNumber)
                    .setBallot(entry.ballot().toProto())
                    .setPhase(entry.phase())
                    .setRequest(entry.request().payload())
                    .build();
            ServerMessage<CommitMessage> commitMessage = new ServerMessage<>(commit);
            executorManager.submitMessageProcessing(() -> triggerCommit(commitMessage));
        } catch (Exception e) {
            logger.error("Error triggering commit for sequence number {}: ", sequenceNumber, e);
        }
    }

    public void triggerCommit(ServerMessage<CommitMessage> commitMessage) {
        try {
            if (state.commitRequest(commitMessage)) {
                promiseTimer.stop();
                tpcHooks.onPaxosCommit(commitMessage);
                if (state.isLeader()) messageSender.broadcastCommit(commitMessage);
            }
        } catch (Exception e) {
            logger.error("Error handling commit message {}: ", commitMessage, e);
        }
    }

    public void refreshTimerOnExecute(String requestId, boolean restart, List<String> txIds, Runnable onStoppingTimer) {
        if (restart) {
            state.hasRequestsWaitingToExecute();
            logger.info("Pending locks detected; restarting client request timer after executing request id: {}, for pending request id: {}", requestId, txIds.get(0));
            clientRequestTimer.restart("refreshing timer on execute for pending request id: " + txIds.get(0));
        } else {
            logger.info("No pending locks to execute; stopping client request timer after executing request id: {}", requestId);
            clientRequestTimer.stop();
            if (isLeader()) onStoppingTimer.run();
        }
    }

    public void promiseTimerCallback() {
        state.runSync(() -> {
            if (state.isCandidate()) {
                logger.info("Promise timer expired while in CANDIDATE role. Re-initiating leader election.");
                leaderElectionInProgress.set(false);
                initiateLeaderElection();
            }
        });
    }

    public void clientRequestTimerCallback() {
        logger.info("Client request timer expired. Initiating leader election.");
        initiateLeaderElection();
    }

    public void reset() {
        metricsListener.printMetrics();
        metricsListener.reset();
        promiseTimer.stop();
        clientRequestTimer.stop();
        leaderElectionInProgress.set(false);
    }
}