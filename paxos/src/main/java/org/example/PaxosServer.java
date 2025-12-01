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
import org.example.state.OperationLog;
import org.example.state.OperationLogEntry;
import org.example.state.PaxosState;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class PaxosServer {
    private static final Logger logger = LogManager.getLogger(PaxosServer.class);

    private final ExecutorManager executorManager;

    private final LivenessTimer promiseTimer;
    private final LivenessTimer clientRequestTimer;

    private final PaxosState state;
    private TPCHooks tpcHooks;
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

    public PaxosServer(int serverId, ExecutorManager executorManager, TPCHooks tpcHooks) {
        this.executorManager = executorManager;
        this.promiseTimer = new LivenessTimer(getRandom(Config.getServerTimeoutMillis() / 3), this::promiseTimerCallback);
        this.clientRequestTimer = new LivenessTimer(Config.getServerTimeoutMillis(), this::clientRequestTimerCallback);

        this.state = new PaxosState(serverId, executorManager.getStateExecutor(), this::onNewClientRequest);
        this.leaderElectionInProgress = new AtomicBoolean(false);

        this.paxosService = new PaxosService(this);
        this.messageSender = new PaxosMessageSender(serverId, executorManager.getNetworkExecutor());

        this.clientRequestHandler = new ClientRequestHandler(state, messageSender);
        this.prepareHandler = new PrepareHandler(state, promiseTimer);
        this.promiseHandler = new PromiseHandler(state, promiseTimer, this::triggerNewView);
        this.newViewHandler = new NewViewHandler(state, promiseTimer);
        this.acceptHandler = new AcceptHandler(state, clientRequestTimer);
        this.acceptedHandler = new AcceptedHandler(state, this::triggerCommit);
        this.commitHandler = new CommitHandler(state, this::triggerCommit);

        this.tpcHooks = tpcHooks;

        logger.info("PaxosServer {} initialized.", serverId);
    }

    private long getRandom(long num) {
        Random random = new Random();
        long max = num + 100;
        long min = num - 100;
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
        clientRequestTimer.startIfNotRunning();
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

    public void onNewClientRequest(ServerMessage<ClientRequest> request) {
        tpcHooks.onNewClientRequest(request);
    }

    public void initiateLeaderElection() {
        if (!leaderElectionInProgress.get()) {
            leaderElectionInProgress.set(true);
            state.runSync(() -> {
                state.transitionToCandidate();
                Ballot ballot = state.getBallot().toProto();
                PrepareMessage prepareMessage = PrepareMessage.newBuilder()
                        .setBallot(ballot)
                        .build();
                messageSender.broadcastPrepare(new ServerMessage<>(prepareMessage), promiseHandler.handler());
                state.setSentPrepare(true);
            });
        }
    }

    public void triggerNewView(org.example.state.Ballot newBallot) {
        if (state.checkBallotAndTransitionToLeader(newBallot)) {
            promiseTimer.stop();
            ServerMessage<NewViewMessage> newView = new ServerMessage<>(state.getNewView());
            tpcHooks.onPaxosNewView(newView); // refactor to be directly called from the operationLog loop
            messageSender.broadcastNewView(newView, acceptedHandler.handler());
            executorManager.submitMessageProcessing(tpcHooks::onRoleChangeToLeader);
            leaderElectionInProgress.set(false);
        }
    }

    public void triggerAccept(ServerMessage<ClientRequest> request, Phase phase) {
        long seqNum = state.acceptRequest(request, phase);
        clientRequestTimer.startIfNotRunning();
        if (state.isLeader()) {
            AcceptMessage acceptMessage = AcceptMessage.newBuilder()
                    .setSequenceNumber(seqNum)
                    .setBallot(state.getBallot().toProto())
                    .setPhase(phase)
                    .setRequest(request.payload())
                    .build();
            messageSender.broadcastAccept(new ServerMessage<>(acceptMessage), acceptedHandler.handler());
        }
    }

    public void triggerAccept(ServerMessage<ClientRequest> request, Phase phase, long seqNum) {
        state.acceptRequestWithSeqNum(request, phase, seqNum);
        clientRequestTimer.startIfNotRunning();
        if (state.isLeader()) {
            AcceptMessage acceptMessage = AcceptMessage.newBuilder()
                    .setSequenceNumber(seqNum)
                    .setBallot(state.getBallot().toProto())
                    .setPhase(phase)
                    .setRequest(request.payload())
                    .build();
            messageSender.broadcastAccept(new ServerMessage<>(acceptMessage), acceptedHandler.handler());
        }
    }

    public void triggerCommit(long sequenceNumber) {
        OperationLogEntry entry = state.getLogEntry(sequenceNumber);
        CommitMessage commit = CommitMessage.newBuilder()
                .setSequenceNumber(sequenceNumber)
                .setBallot(entry.ballot().toProto())
                .setPhase(entry.phase())
                .setRequest(entry.request().payload())
                .build();
        ServerMessage<CommitMessage> commitMessage = new ServerMessage<>(commit);
        triggerCommit(commitMessage);
    }

    public void triggerCommit(ServerMessage<CommitMessage> commitMessage) {
        if (state.commitRequest(commitMessage)) {
            tpcHooks.onPaxosCommit(commitMessage);
            if (state.isLeader()) messageSender.broadcastCommit(commitMessage);
        }
    }

    public void refreshTimerOnExecute() {
        if (state.hasRequestsWaitingToExecute()) {
            logger.info("Pending client requests detected; restarting client request timer.");
            clientRequestTimer.restart();
        } else {
            logger.info("No pending client requests to execute; stopping client request timer.");
            clientRequestTimer.stop();
        }
    }

    public void promiseTimerCallback() {
        state.runSync(() -> {
            if (state.isCandidate()) {
                logger.info("Promise timer expired while in CANDIDATE role. Re-initiating leader election.");
                initiateLeaderElection();
            }
        });
    }

    public void clientRequestTimerCallback() {
        logger.info("Client request timer expired. Initiating leader election.");
        initiateLeaderElection();
    }
}