package org.example.messaging;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.ClientReply;
import org.example.ClientRequest;
import org.example.ClientServiceGrpc;
import org.example.tpc.TPCServer;

public class ClientService extends ClientServiceGrpc.ClientServiceImplBase {
    private static final Logger logger = LogManager.getLogger(ClientService.class);

    private final TPCServer server;

    public ClientService(TPCServer server) {
        this.server = server;
    }

    @Override
    public void request(ClientRequest request, StreamObserver<ClientReply> responseObserver) {
        ServerMessage<ClientRequest> message = new ServerMessage<>(request);
        logger.info("Received client request: {}", message);
        server.handleClientRequest(message, responseObserver);
    }

    @Override
    public void ping(Empty request, StreamObserver<Empty> responseObserver) {
        logger.debug("Received ping request");
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }
}
