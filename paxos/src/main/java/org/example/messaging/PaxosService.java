package org.example.messaging;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.state.Ballot;

public class PaxosService extends PaxosServiceGrpc.PaxosServiceImplBase {
    private static final Logger logger = LogManager.getLogger(PaxosService.class);

    private PaxosServer server;

    public PaxosService(PaxosServer server) {
        this.server = server;
    }

    public synchronized void reset(PaxosServer server) {
        this.server = server;
    }

    @Override
    public void prepare(PrepareMessage request, StreamObserver<PromiseMessage> responseObserver) {
        ServerMessage<PrepareMessage> message = new ServerMessage<>(request);
        logger.info("Received PrepareMessage with ballot number: {}", new Ballot(request.getBallot()));
        server.handlePrepare(message, responseObserver);
    }

    public void newView(NewViewMessage request, StreamObserver<AcceptedMessage> responseObserver) {
        ServerMessage<NewViewMessage> message = new ServerMessage<>(request);
        logger.info("Received NewViewMessage : {}", message);
        server.handleNewView(message, responseObserver);
    }

    public void accept(AcceptMessage request, StreamObserver<AcceptedMessage> responseObserver) {
        ServerMessage<AcceptMessage> message = new ServerMessage<>(request);
        logger.info("Received Accept message: {}", message);
        server.handleAccept(message, responseObserver);
    }

    public void commit(CommitMessage request, StreamObserver<Empty> responseObserver) {
        ServerMessage<CommitMessage> message = new ServerMessage<>(request);
        logger.info("Received Commit message: {}", message);
        server.handleCommit(message);
    }

    @Override
    public void ping(Empty request, StreamObserver<Empty> responseObserver) {
        logger.debug("Received ping request");
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }
}
