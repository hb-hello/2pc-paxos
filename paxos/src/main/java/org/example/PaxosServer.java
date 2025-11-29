package org.example;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.config.Config;
import org.example.consensus.LivenessTimer;
import org.example.consensus.handlers.AcceptedHandler;
import org.example.consensus.handlers.ClientRequestHandler;
import org.example.consensus.handlers.PrepareHandler;
import org.example.consensus.handlers.PromiseHandler;
import org.example.messaging.PaxosMessageSender;
import org.example.messaging.PaxosService;
import org.example.messaging.ServerMessage;
import org.example.state.PaxosState;
import org.example.state.Role;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class PaxosServer {
    private static final Logger logger = LogManager.getLogger(PaxosServer.class);

    private final ExecutorManager executorManager;

    private final LivenessTimer promiseTimer;
    private final LivenessTimer clientRequestTimer;

    private final PaxosState state;
    private AtomicBoolean leaderElectionInProgress;

    private final PaxosService paxosService;
    private final PaxosMessageSender messageSender;

    private final ClientRequestHandler clientRequestHandler;
    private final PrepareHandler prepareHandler;
    private final PromiseHandler promiseHandler;
    private final AcceptedHandler acceptedHandler;

    public PaxosServer(int serverId, ExecutorManager executorManager) {
        this.executorManager = executorManager;
        this.promiseTimer = new LivenessTimer(getRandom(Config.getServerTimeoutMillis() / 3), this::promiseTimerCallback);
        this.clientRequestTimer = new LivenessTimer(Config.getServerTimeoutMillis(), this::clientRequestTimerCallback);

        this.state = new PaxosState(serverId, executorManager.getStateExecutor());
        this.leaderElectionInProgress = new AtomicBoolean(false);

        this.paxosService = new PaxosService(this);
        this.messageSender = new PaxosMessageSender(serverId, executorManager.getNetworkExecutor(), executorManager.getMessageExecutor());

        this.clientRequestHandler = new ClientRequestHandler(state, messageSender);
        this.prepareHandler = new PrepareHandler(state, promiseTimer);
        this.promiseHandler = new PromiseHandler(state, promiseTimer, this::triggerNewView);
        this.acceptedHandler = new AcceptedHandler(state);

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

    public void handleClientRequest(ServerMessage<ClientRequest> request) {
        executorManager.submitMessageProcessing(() -> clientRequestHandler.handle(request, this::initiateLeaderElection));
    }

    public void handlePrepare(ServerMessage<PrepareMessage> prepareMessage, StreamObserver<PromiseMessage> responseObserver) {
        executorManager.submitMessageProcessing(() -> prepareHandler.handle(prepareMessage, responseObserver));
    }

    public void handleNewView(NewViewMessage newView) {
        promiseTimer.stop();
        state.transitionToBackup();
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
            NewViewMessage newView = state.constructNewView();
            messageSender.broadcastNewView(newView, acceptedHandler.handler());
            leaderElectionInProgress.set(false);
        }
    }

    public void promiseTimerCallback() {
        state.runSync(() -> {
            if (state.getRole() == Role.CANDIDATE && !leaderElectionInProgress.get()) {
                initiateLeaderElection();
            }
        });
    }

    public void clientRequestTimerCallback() {
        initiateLeaderElection();
    }
}