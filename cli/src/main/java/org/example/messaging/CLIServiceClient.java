package org.example.messaging;

import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.CLIServiceGrpc;
import org.example.CliMain;

import java.util.concurrent.atomic.AtomicBoolean;

public class CLIServiceClient extends CLIServiceGrpc.CLIServiceImplBase {
    private static final Logger logger = LogManager.getLogger(CLIServiceClient.class);

    private final CliMain cli;
    private final AtomicBoolean warmedUp;

    public CLIServiceClient(CliMain cli) {
        this.cli = cli;
        this.warmedUp = new AtomicBoolean(false);
    }

    @Override
    public void ping(Empty request, StreamObserver<Empty> responseObserver) {
        logger.debug("Received ping request");
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
        synchronized (warmedUp) {
            if (!warmedUp.get()) {
                logger.debug("Warming up CLI client after receiving first ping");
                cli.warmupWithPings();
                warmedUp.set(true);
            }
        }
    }
}
