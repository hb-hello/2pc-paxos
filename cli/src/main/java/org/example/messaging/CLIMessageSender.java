package org.example.messaging;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;

import java.util.concurrent.ExecutorService;

public class CLIMessageSender extends MessageSender {
    private static final Logger logger = LogManager.getLogger(CLIMessageSender.class);

    private final ExecutorService networkExecutor;

    public CLIMessageSender(int clientId, ExecutorService networkExecutor) {
        super(clientId, networkExecutor);
        this.networkExecutor = networkExecutor;
    }

    public void sendClientRequest(int targetServerId, ClientRequest request) {
        ensureActive();
        ClientServiceGrpc.ClientServiceFutureStub stub = stubManager.getClientStub(targetServerId);
        ListenableFuture<ClientReply> replyFuture  = stub.request(request);
        replyFuture.addListener(() -> {
            try {
                ClientReply reply = replyFuture.get();
                logger.info("Received reply from server {}: {}", targetServerId, reply);
            } catch (Exception e) {
                logger.error("Failed to get reply from server {}", targetServerId, e);
            }
        }, networkExecutor);
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
}
