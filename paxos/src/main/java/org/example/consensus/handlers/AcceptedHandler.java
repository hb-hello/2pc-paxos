package org.example.consensus.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.AcceptedMessage;
import org.example.PromiseMessage;
import org.example.messaging.ServerMessage;
import org.example.state.PaxosState;

public class AcceptedHandler {
    private static final Logger logger = LogManager.getLogger(AcceptedHandler.class);

    private final PaxosState state;

    public AcceptedHandler(PaxosState state) {
        this.state = state;
    }

    public void handle(ServerMessage<AcceptedMessage> message) {

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
