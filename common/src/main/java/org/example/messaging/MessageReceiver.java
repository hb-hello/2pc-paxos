package org.example.messaging;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MessageReceiver {

    private static final Logger logger = LogManager.getLogger(MessageReceiver.class);

    private final int nodeId;
    private final int port;
    private final Server grpcServer;
    private final ServerActivityInterceptor interceptor;

    // Constructor accepting multiple services and an interceptor
    public MessageReceiver(int nodeId, int port,
                              List<BindableService> services, ServerActivityInterceptor interceptor) {
        this.nodeId = nodeId;
        this.port = port;
        this.interceptor = interceptor;
        ServerBuilder<?> builder = ServerBuilder.forPort(port);
        if (services != null) {
        logger.info("Adding {} services to GRPC server for node {}", services.size(), nodeId);
            for (BindableService svc : services) {
                builder.addService(svc);
            }
        }
        if (interceptor != null) {
            builder.intercept(interceptor);
        }
        this.grpcServer = builder.build();
        logger.info("GRPC server for node {} initialized on port {}", nodeId, port);
    }

    // Overloaded constructor without interceptor parameter
    public MessageReceiver(int nodeId, int port,
                              List<BindableService> services) {
        this.nodeId = nodeId;
        this.port = port;
        this.interceptor = null;

        ServerBuilder<?> builder = ServerBuilder.forPort(port);
        if (services != null) {
            for (BindableService svc : services) {
                builder.addService(svc);
            }
        }
        this.grpcServer = builder.build();
    }

    public void setActive(boolean active) {
        if (interceptor != null) {
            interceptor.setActiveFlag(active);
        }
    }

    public void startListening(Runnable onStartCallback) {
        try {
            grpcServer.start();
            logger.info("GRPC Server for node {} started listening on port {}",
                    nodeId, port);
            if (onStartCallback != null) {
                onStartCallback.run();
            }
            grpcServer.awaitTermination();
        } catch (IOException e) {
            logger.error("Node {}: Error in starting GRPC server : {}", nodeId, e.getMessage());
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            logger.error("Node {}: GRPC server interrupted : {}", nodeId, e.getMessage());
            logger.info("GRPC server was shut down.");
            throw new RuntimeException(e);
        }
    }

    public void shutdown() {
        grpcServer.shutdown();
        try {
            if (!grpcServer.awaitTermination(5, TimeUnit.SECONDS)) {
                grpcServer.shutdownNow();
            }
        } catch (InterruptedException e) {
            grpcServer.shutdownNow();
        }
    }
}
