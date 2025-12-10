package org.example.messaging;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.tpc.TPCServer;

public class TPCService extends TPCServiceGrpc.TPCServiceImplBase {
    private static final Logger logger = LogManager.getLogger(TPCService.class);

    private TPCServer server;

    public TPCService(TPCServer server) {
        this.server = server;
    }

    public synchronized void reset(TPCServer server) {
        this.server = server;
    }

    @Override
    public void tPCPrepare(TPCPrepareMessage request, StreamObserver<TPCAckMessage> responseObserver) {
        ServerMessage<TPCPrepareMessage> message = new ServerMessage<>(request);
        logger.info("Received TPCPrepare message : {}", message);
        server.handlePrepare(message, responseObserver);
    }

    @Override
    public void tPCPrepared(TPCPreparedMessage request, StreamObserver<Empty> responseObserver) {
        ServerMessage<TPCPreparedMessage> message = new ServerMessage<>(request);
        logger.info("Received TPCPrepared message : {}", message);
        server.handlePrepared(message);
    }

    @Override
    public void tPCCommit(TPCCommitMessage request, StreamObserver<TPCAckMessage> responseObserver) {
        ServerMessage<TPCCommitMessage> message = new ServerMessage<>(request);
        logger.info("Received TPCCommit message : {}", message);
        server.handleCommit(message, responseObserver);
    }

    @Override
    public void tPCAbort(TPCAbortMessage request, StreamObserver<TPCAckMessage> responseObserver) {
        ServerMessage<TPCAbortMessage> message = new ServerMessage<>(request);
        logger.info("Received TPCAbort message : {}", message);
        server.handleAbort(message, responseObserver);
    }

    @Override
    public void leaderElected(NewLeader request, StreamObserver<Empty> responseObserver) {
        logger.info("Received NewLeader message from server : {}", request.getSenderId());
        server.handleLeaderElected(request);
    }

    @Override
    public void ping(Empty request, StreamObserver<Empty> responseObserver) {
        logger.debug("Received ping request");
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }
}
