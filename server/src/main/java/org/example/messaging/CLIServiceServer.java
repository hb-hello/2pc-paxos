package org.example.messaging;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;

public class CLIServiceServer extends CLIServiceGrpc.CLIServiceImplBase {
    private static final Logger logger = LogManager.getLogger(CLIServiceServer.class);

    private final ServerMain server;

    public CLIServiceServer(ServerMain server) {
        this.server = server;
    }

    @Override
    public void setActiveFlag(ActiveFlag request, StreamObserver<Acknowledgement> responseObserver) {
        server.setActive(request.getActiveFlag());
        logger.info("Set active flag to: {}", request.getActiveFlag());
        Acknowledgement ack = Acknowledgement.newBuilder().setStatus(true).build();
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

    @Override
    public void failNode(NodeId nodeId, StreamObserver<Acknowledgement> responseObserver) {
        server.setActive(false);
        logger.info("Fail server called from CLI");
        Acknowledgement ack = Acknowledgement.newBuilder().setStatus(true).build();
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

    @Override
    public void recoverNode(NodeId nodeId, StreamObserver<Acknowledgement> responseObserver) {
        server.setActive(true);
        logger.info("Recover server called from CLI");
        Acknowledgement ack = Acknowledgement.newBuilder().setStatus(true).build();
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

    public void reset(Empty request, StreamObserver<Acknowledgement> responseObserver) {
        logger.info("Reset server called from CLI");
        server.reset();
        Acknowledgement ack = Acknowledgement.newBuilder().setStatus(true).build();
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

    @Override
    public void getDB(Empty request, StreamObserver<CLIResponse> responseObserver) {
        logger.info("Received getDB request");
        String dbContent = server.getDB();
        CLIResponse response = CLIResponse.newBuilder().setCliResponse(dbContent).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getLog(Empty request, StreamObserver<CLIResponse> responseObserver) {
        logger.info("Received getLog request");
        String logContent = server.getOperationLog();
        CLIResponse response = CLIResponse.newBuilder().setCliResponse(logContent).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getRequests(Empty request, StreamObserver<CLIResponse> responseObserver) {
        logger.info("Received getRequests request");
        String logContent = server.getTrackedRequests();
        CLIResponse response = CLIResponse.newBuilder().setCliResponse(logContent).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void getNewViews(Empty request, StreamObserver<CLIResponse> responseObserver) {
        logger.info("Received getNewViews request");
        for (NewViewMessage nvm : server.getNewViews()) {
            CLIResponse response = CLIResponse.newBuilder().setCliResponse(ServerMessageFormatter.formatNewViewDetailed(nvm)).build();
            responseObserver.onNext(response);
            logger.info("Sent NewViewMessage: {}", ServerMessageFormatter.formatNewViewDetailed(nvm));
        }
        responseObserver.onCompleted();
    }

    @Override
    public void ping(Empty request, StreamObserver<Empty> responseObserver) {
        logger.debug("Received ping request");
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }
}
