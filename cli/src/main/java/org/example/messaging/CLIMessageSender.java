package org.example.messaging;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Empty;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.config.Config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class CLIMessageSender extends MessageSender {
    private static final Logger logger = LogManager.getLogger(CLIMessageSender.class);

    private final ExecutorService networkExecutor;

    public CLIMessageSender(int clientId, ExecutorService networkExecutor) {
        super(clientId, networkExecutor);
        this.networkExecutor = networkExecutor;
    }

    public ListenableFuture<ClientReply> sendClientRequestWithDeadline(int targetServerId, ClientRequest request, long deadlineMillis) {
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
}
