package org.example.consensus.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.AcceptedMessage;
import org.example.PromiseMessage;
import org.example.config.Config;
import org.example.messaging.ServerMessage;
import org.example.state.Ballot;
import org.example.state.PaxosState;

import java.util.function.Consumer;

public class AcceptedHandler {
    private static final Logger logger = LogManager.getLogger(AcceptedHandler.class);

    private final PaxosState state;
    private final Consumer<Long> onQuorumReached;

    public AcceptedHandler(PaxosState state, Consumer<Long> onQuorumReached) {
        this.state = state;
        this.onQuorumReached = onQuorumReached;
    }

    public void handle(ServerMessage<AcceptedMessage> accepted) {
        logger.info("Received accepted message : {}", accepted);
        if (state.trackMessageWithConsensus(accepted, Config.getQuorumSize() - 1)) {
            logger.info("Accepted messages have reached a quorum of {} for sequence number {}",
                    Config.getQuorumSize() - 1,
                    accepted.payload().getSequenceNumber());
            onQuorumReached.accept(accepted.payload().getSequenceNumber());
        }
    }

    public StreamObserver<AcceptedMessage> handler() {
        return new StreamObserver<>() {
            @Override
            public void onNext(AcceptedMessage acceptedMessage) {
                handle(new ServerMessage<>(acceptedMessage));
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
