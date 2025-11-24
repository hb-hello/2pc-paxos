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
//        server.setActive(request.getActiveFlag());
        logger.info("Set active flag to: {}", request.getActiveFlag());
        Acknowledgement ack = Acknowledgement.newBuilder().setStatus(true).build();
        responseObserver.onNext(ack);
        responseObserver.onCompleted();
    }

    @Override
    public void ping(Empty request, StreamObserver<Empty> responseObserver) {
        logger.debug("Received ping request");
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }
}
