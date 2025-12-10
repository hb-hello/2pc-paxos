package org.example.tpc.handlers;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.ClientRequest;
import org.example.TPCAckMessage;
import org.example.TPCPrepareMessage;
import org.example.messaging.ServerMessage;
import org.example.tpc.ClientRequestTracker;

import java.util.function.Consumer;

public class PrepareHandler {
    private static final Logger logger = LogManager.getLogger(PrepareHandler.class);

    private final ClientRequestTracker clientRequestTracker;
    private final Consumer<String> sendPrepared;
    private final Consumer<ServerMessage<ClientRequest>> sendAbort;
    private final Consumer<ServerMessage<ClientRequest>> requestHandler;

    public PrepareHandler(ClientRequestTracker clientRequestTracker,
                          Consumer<String> sendPrepared,
                          Consumer<ServerMessage<ClientRequest>> sendAbort,
                          Consumer<ServerMessage<ClientRequest>> requestHandler) {
        this.clientRequestTracker = clientRequestTracker;
        this.sendPrepared = sendPrepared;
        this.sendAbort = sendAbort;
        this.requestHandler = requestHandler;
    }

    public void handle(ServerMessage<TPCPrepareMessage> prepare, StreamObserver<TPCAckMessage> responseObserver) {
        ServerMessage<ClientRequest> request = new ServerMessage<>(prepare.payload().getClientRequest());

        if (clientRequestTracker.isAccepted(request)) {
            responseObserver.onNext(TPCAckMessage.newBuilder().build());
            responseObserver.onCompleted();
            logger.info("Received duplicate prepare for client request {}", request.getMessageId());
            if (clientRequestTracker.isPrepared(request) || clientRequestTracker.isCommitted(request)) {
                logger.info("Client request {} is already prepared / committed. Resending prepared message.", request.getMessageId());
                sendPrepared.accept(request.getMessageId());
            } else if (clientRequestTracker.isAborted(request)) {
                logger.info("Client request {} is already aborted. Resending abort message.", request.getMessageId());
                sendAbort.accept(request);
            }
        } else {
            logger.info("Processing client request in prepare: {}", request.getMessageId());
            requestHandler.accept(request);
        }
    }
}
