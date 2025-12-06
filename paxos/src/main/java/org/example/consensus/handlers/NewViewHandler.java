package org.example.consensus.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.AcceptMessage;
import org.example.AcceptedMessage;
import org.example.CommitMessage;
import org.example.NewViewMessage;
import org.example.consensus.LivenessTimer;
import org.example.messaging.ServerMessage;
import org.example.state.Ballot;
import org.example.state.PaxosState;

public class NewViewHandler {
    private static final Logger logger = LogManager.getLogger(NewViewHandler.class);

    private final PaxosState state;
    private final LivenessTimer promiseTimer;
    private final Runnable onCheckpointSeen;

    public NewViewHandler(PaxosState state, LivenessTimer promiseTimer, Runnable onCheckpointSeen) {
        this.state = state;
        this.promiseTimer = promiseTimer;
        this.onCheckpointSeen = onCheckpointSeen;
    }

    public void handle(ServerMessage<NewViewMessage> newViewMessage, StreamObserver<AcceptedMessage> responseObserver) {
        NewViewMessage newView = newViewMessage.payload();
        Ballot newViewBallot = new Ballot(newView.getBallot());

        if (state.checkBallotAndTransitionToBackup(newViewBallot)) {
            promiseTimer.stop();

            long checkpointSeqNum = newView.getLatestCheckpointSeq();
            state.setLatestCheckpointSeqSeen(checkpointSeqNum);
            onCheckpointSeen.run();

            for (AcceptMessage acceptMessage : newView.getAcceptLogList()) {
                if (state.acceptRequest(new ServerMessage<>(acceptMessage))) {
                    AcceptedMessage acceptedMessage = AcceptedMessage.newBuilder()
                            .setSequenceNumber(acceptMessage.getSequenceNumber())
                            .setSenderId(state.getServerId())
                            .setBallot(acceptMessage.getBallot())
                            .setPhase(acceptMessage.getPhase())
                            .build();
                    responseObserver.onNext(acceptedMessage);
                }
            }
            for (CommitMessage msg : newView.getCommitLogList()) {
                state.commitRequest(new ServerMessage<>(msg));
            }
            responseObserver.onCompleted();
            logger.info("Completed responding to new view");
        } else {
            logger.info("New view request rejected for ballot number: {}", newViewBallot);
            responseObserver.onCompleted();
        }
    }
}
