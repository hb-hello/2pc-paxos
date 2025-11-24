package org.example.messaging;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.PaxosServer;
import org.example.PaxosServiceGrpc;
import org.example.PrepareMessage;
import org.example.PromiseMessage;

public class PaxosService extends PaxosServiceGrpc.PaxosServiceImplBase {
    private static final Logger logger = LogManager.getLogger(PaxosService.class);

    private final PaxosServer server;

    public PaxosService(PaxosServer server) {
        this.server = server;
    }

    @Override
    public void prepare(PrepareMessage request, StreamObserver<PromiseMessage> responseObserver) {
        logger.info("Received PrepareMessage with ballot number: {}", request.getBallot());

        PromiseMessage response = PromiseMessage.newBuilder()
                .setBallot(request.getBallot())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void ping(Empty request, StreamObserver<Empty> responseObserver) {
        logger.debug("Received ping request");
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }
}
