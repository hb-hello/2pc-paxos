package org.example.messaging;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;

public class PaxosService extends PaxosServiceGrpc.PaxosServiceImplBase {
    private static final Logger logger = LogManager.getLogger(PaxosService.class);

    private final PaxosServer server;

    public PaxosService(PaxosServer server) {
        this.server = server;
    }

    @Override
    public void prepare(PrepareMessage request, StreamObserver<PromiseMessage> responseObserver) {
        ServerMessage<PrepareMessage> message = new ServerMessage<>(request);
        logger.info("Received PrepareMessage with ballot number: {}", request.getBallot());
        server.handlePrepare(message, responseObserver);
    }

    public void newView(NewViewMessage request, StreamObserver<AcceptedMessage> responseObserver) {
        logger.info("Received NewViewMessage for view number: {}", request.getBallot());
        server.handleNewView(request);
    }

    @Override
    public void ping(Empty request, StreamObserver<Empty> responseObserver) {
        logger.debug("Received ping request");
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }
}
