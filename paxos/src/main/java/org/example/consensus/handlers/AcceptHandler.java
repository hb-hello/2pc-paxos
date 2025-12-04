package org.example.consensus.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.AcceptMessage;
import org.example.AcceptedMessage;
import org.example.consensus.LivenessTimer;
import org.example.messaging.ServerMessage;
import org.example.state.Ballot;
import org.example.state.PaxosState;

public class AcceptHandler {
    private static final Logger logger = LogManager.getLogger(AcceptHandler.class);

    private final PaxosState state;
    private final LivenessTimer clientRequestTimer;
    private final LivenessTimer promiseTimer;

    public AcceptHandler(PaxosState state, LivenessTimer clientRequestTimer, LivenessTimer promiseTimer) {
        this.state = state;
        this.clientRequestTimer = clientRequestTimer;
        this.promiseTimer = promiseTimer;
    }

    public void handle(ServerMessage<AcceptMessage> accept, StreamObserver<AcceptedMessage> responseObserver) {
        Ballot acceptBallot = new Ballot(accept.payload().getBallot());

        if (state.checkBallotAndTransitionToBackup(acceptBallot) && state.acceptRequest(accept)) {
            promiseTimer.stop();
            clientRequestTimer.startIfNotRunning("handling accept message " + accept);
            AcceptedMessage acceptedMessage = AcceptedMessage.newBuilder()
                    .setSequenceNumber(accept.payload().getSequenceNumber())
                    .setSenderId(state.getServerId())
                    .setBallot(accept.payload().getBallot())
                    .setPhase(accept.payload().getPhase())
                    .build();
            responseObserver.onNext(acceptedMessage);
            responseObserver.onCompleted();
            logger.info("Sent accepted message : {}", acceptedMessage);
        } else {
            logger.info("Accept request rejected for ballot number: {}", acceptBallot);
//            responseObserver.onCompleted();
        }
    }
}
