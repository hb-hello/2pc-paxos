package org.example.consensus.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.AcceptMessage;
import org.example.CommitMessage;
import org.example.PromiseMessage;
import org.example.config.Config;
import org.example.consensus.LivenessTimer;
import org.example.messaging.ServerMessage;
import org.example.state.Ballot;
import org.example.state.PaxosState;

import java.util.function.Consumer;

public class PromiseHandler {
    private static final Logger logger = LogManager.getLogger(PromiseHandler.class);

    private final PaxosState state;
    private final LivenessTimer promiseTimer;
    private final Consumer<Ballot> onQuorumReached;

    public PromiseHandler(PaxosState state, LivenessTimer promiseTimer, Consumer<Ballot> onQuorumReached) {
        this.state = state;
        this.promiseTimer = promiseTimer;
        this.onQuorumReached = onQuorumReached;
    }

    public void handle(ServerMessage<PromiseMessage> promise) {
        logger.info("Received promise message {}", promise);

        if (state.isLeader()) return; // ignore promises after becoming leader

        promiseTimer.restart("receiving a promise");

        for (AcceptMessage msg : promise.payload().getAcceptLogList()) {
            state.acceptRequest(new ServerMessage<>(msg));
        }

        for (CommitMessage msg : promise.payload().getCommitLogList()) {
            state.commitRequest(new ServerMessage<>(msg));
        }

        state.setLatestCheckpointSeqSeen(promise.payload().getLatestCheckpointSeq());

        if (state.trackMessageWithConsensus(promise, Config.getQuorumSize() - 1)) {
            Ballot ballot = new Ballot(promise.payload().getBallot());
            logger.info("Promise messages have reached a quorum of {} for ballot {}",
                    Config.getQuorumSize() - 1,
                    ballot);
            onQuorumReached.accept(ballot);
        }
    }

    public StreamObserver<PromiseMessage> handler() {
        return new StreamObserver<>() {
            @Override
            public void onNext(PromiseMessage promiseMessage) {
                handle(new ServerMessage<>(promiseMessage));
            }

            @Override
            public void onError(Throwable throwable) {
                logger.error("Error receiving PromiseMessage: {}", throwable.getMessage());
            }

            @Override
            public void onCompleted() {
//                logger.info("Completed receiving PromiseMessages");
            }
        };
    }
}
