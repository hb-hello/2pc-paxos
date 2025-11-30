package org.example.messaging;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.CliMain;
import org.example.ClientReply;
import org.example.ClientServiceGrpc;
import org.example.client.ClientNode;

public class ClientServiceClient extends ClientServiceGrpc.ClientServiceImplBase {
    private static final Logger logger = LogManager.getLogger(ClientServiceClient.class);

    private final CliMain cli;

    public ClientServiceClient(CliMain cli) {
        this.cli = cli;
    }

    @Override
    public void reply(ClientReply request, StreamObserver<Empty> responseObserver) {
        cli.handleClientReply(request);
    }
}
