package org.example.consensus.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.PrepareMessage;
import org.example.PromiseMessage;
import org.example.consensus.LivenessTimer;
import org.example.messaging.ServerMessage;
import org.example.state.Ballot;
import org.example.state.OperationLog;
import org.example.state.PaxosState;

public class PrepareHandler {
    private static final Logger logger = LogManager.getLogger(PrepareHandler.class);

    private final PaxosState state;
    private final LivenessTimer promiseTimer;

    public PrepareHandler(PaxosState state, LivenessTimer promiseTimer) {
        this.state = state;
        this.promiseTimer = promiseTimer;

    }

    public void handle(ServerMessage<PrepareMessage> prepare, StreamObserver<PromiseMessage> responseObserver) {
        logger.info("Received Prepare message: {}", prepare);
        Ballot newBallot = new Ballot(prepare.payload().getBallot());
        if (state.updateBallot(newBallot)) {
            PromiseMessage promise = state.getPromiseMessage().toBuilder()
                    .setBallot(newBallot.toProto())
                    .setSenderId(state.getServerId())
                    .build();
            responseObserver.onNext(promise);
            responseObserver.onCompleted();
            logger.info("Sent Promise message: {}", promise);
        }
    }
}
