package org.example.messaging;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.config.Config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

public class CLIMessageSender extends MessageSender {
    private static final Logger logger = LogManager.getLogger(CLIMessageSender.class);

    public CLIMessageSender(int clientId) {
        super(clientId);
    }

    public ListenableFuture<Empty> sendClientRequestWithDeadline(int targetServerId, ClientRequest request, long deadlineMillis) {
        ensureActive();
        ClientServiceGrpc.ClientServiceFutureStub stub = stubManager.getClientStub(targetServerId).withDeadlineAfter(deadlineMillis, TimeUnit.MILLISECONDS);
        return stub.request(request);
    }

    public void failNode(NodeId nodeId) {
        ensureActive();
        CLIServiceGrpc.CLIServiceBlockingStub stub = stubManager.getCLIBlockingStub(nodeId.getNodeId());
        Acknowledgement ack = stub.failNode(nodeId);
        if (ack.getStatus()) {
            logger.info("Server {} acknowledged fail node {}", nodeId.getNodeId(), nodeId.getNodeId());
        } else {
            throw new RuntimeException("Server " + nodeId.getNodeId() + " did not acknowledge fail node " + nodeId.getNodeId());
        }
    }

    public void recoverNode(NodeId nodeId) {
        ensureActive();
        CLIServiceGrpc.CLIServiceBlockingStub stub = stubManager.getCLIBlockingStub(nodeId.getNodeId());
        Acknowledgement ack = stub.recoverNode(nodeId);
        if (ack.getStatus()) {
            logger.info("Server {} acknowledged recover node {}", nodeId.getNodeId(), nodeId.getNodeId());
        } else {
            throw new RuntimeException("Server " + nodeId.getNodeId() + " did not acknowledge recover node " + nodeId.getNodeId());
        }
    }

    public void sendActiveFlag(int targetServerId, boolean isActive) {
        ensureActive();
        CLIServiceGrpc.CLIServiceBlockingStub stub = stubManager.getCLIBlockingStub(targetServerId);
        ActiveFlag activeFlag = ActiveFlag.newBuilder()
                .setActiveFlag(isActive)
                .build();
        Acknowledgement ack = stub.setActiveFlag(activeFlag);
        if (ack.getStatus()) {
            logger.info("Server {} acknowledged active flag {}", targetServerId, isActive);
        } else {
            throw new RuntimeException("Server " + targetServerId + " did not acknowledge active flag " + isActive);
        }
    }

    public void sendReset(int targetServerId) {
        ensureActive();
        CLIServiceGrpc.CLIServiceBlockingStub stub = stubManager.getCLIBlockingStub(targetServerId);
        Acknowledgement ack = stub.reset(Empty.getDefaultInstance());
        if (ack.getStatus()) {
            logger.info("Server {} acknowledged reset", targetServerId);
        } else {
            throw new RuntimeException("Server " + targetServerId + " did not acknowledge reset");
        }
    }

    public void printDB() {
        // Collect responses for all servers, preserving order
        Map<String, CLIResponse> responses = new LinkedHashMap<>();
        for (int serverId = 1; serverId <= Config.getServerCount(); serverId++) {
            try {
                CLIResponse response =
                        stubManager.getCLIBlockingStub(serverId).getDB(Empty.getDefaultInstance());
                responses.put(String.valueOf(serverId), response);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // Print a single consolidated table for all server responses
        CLIFormatter.printDBAsTable(responses);
    }

    public void printOperationLog(int serverId) {
        // Collect responses for all servers, preserving order
        try {
            CLIResponse response =
                    stubManager.getCLIBlockingStub(serverId).getLog(Empty.getDefaultInstance());
            System.out.println(response.getCliResponse());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void printTrackedRequests(int serverId) {
        // Collect responses for all servers, preserving order
        try {
            CLIResponse response =
                    stubManager.getCLIBlockingStub(serverId).getRequests(Empty.getDefaultInstance());
            System.out.println(response.getCliResponse());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void getNewViews(StreamObserver<CLIResponse> responseObserver) {
        int serverCount = Config.getServerCount();
        CountDownLatch latch = new CountDownLatch(serverCount);
        java.util.concurrent.atomic.AtomicReference<Throwable> firstError = new java.util.concurrent.atomic.AtomicReference<>();

        for (int serverId = 1; serverId <= serverCount; serverId++) {
            try {
                // Wrap the provided observer so we can detect completion per-stream
                StreamObserver<CLIResponse> wrapper = new StreamObserver<>() {
                    @Override
                    public void onNext(CLIResponse value) {
                        try {
                            responseObserver.onNext(value);
                        } catch (Throwable t) {
                            logger.warn("Wrapped responseObserver.onNext threw: {}", t.getMessage());
                            firstError.compareAndSet(null, t);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        firstError.compareAndSet(null, t);
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                };

                stubManager.getCLIAsyncStub(serverId).getNewViews(Empty.getDefaultInstance(), wrapper);
            } catch (Exception e) {
                // If one individual call fails to start, record the error and count down so latch can proceed
                logger.warn("Failed to start getNewViews stream for server {}: {}", serverId, e.getMessage());
                firstError.compareAndSet(null, e);
                latch.countDown();
            }
        }

        // Block until all streams complete or timeout. Use a safe default timeout to avoid hanging indefinitely.
        boolean finished = false;
        try {
            finished = latch.await(Math.max(5, Config.getClientTimeoutMillis() / 1000), TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            firstError.compareAndSet(null, ie);
            finished = false;
        }

        // Deliver a single terminal signal to the provided observer: either onError (first seen) or onCompleted
        Throwable err = firstError.get();
        if (err != null) {
            try {
                responseObserver.onError(err);
            } catch (Throwable ignore) {
            }
            return;
        }

        if (!finished) {
            RuntimeException rte = new RuntimeException("Timed out waiting for getNewViews streams to complete");
            try {
                responseObserver.onError(rte);
            } catch (Throwable ignore) {
            }
            return;
        }

        try {
            responseObserver.onCompleted();
        } catch (Throwable ignore) {
        }
    }
}
