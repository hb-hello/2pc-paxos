package org.example.messaging;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.config.Config;

import java.util.concurrent.ExecutorService;

public class PaxosMessageSender extends MessageSender {

    private static final Logger logger = LogManager.getLogger(PaxosMessageSender.class);

    public PaxosMessageSender(int nodeId, ExecutorService networkExecutor) {
        super(nodeId);
    }

    public void forwardClientRequest(int targetNodeId, ServerMessage<ClientRequest> message) {
        ensureActive();
        logger.info("Forwarding client request to Paxos node {} : {}", targetNodeId, message);
        stubManager.getClientStub(targetNodeId).request(message.payload());
    }

    public void broadcastPrepare(ServerMessage<PrepareMessage> message, StreamObserver<PromiseMessage> responseObserver) {
        ensureActive();
        logger.info("Broadcasting prepare message to all Paxos nodes: {}", message);
        for (int serverId : Config.getServerIdsInClusterExcept(nodeId)) {
            stubManager.getPaxosAsyncStub(serverId).prepare(message.payload(), responseObserver);
        }
    }

    public void broadcastNewView(ServerMessage<NewViewMessage> message, StreamObserver<AcceptedMessage> responseObserver) {
        ensureActive();
        logger.info("Broadcasting new view message to all Paxos nodes: {}", message);
        for (int serverId : Config.getServerIdsInClusterExcept(nodeId)) {
            stubManager.getPaxosAsyncStub(serverId).newView(message.payload(), responseObserver);
        }
    }

    public void broadcastAccept(ServerMessage<AcceptMessage> message, StreamObserver<AcceptedMessage> responseObserver) {
        ensureActive();
        logger.info("Broadcasting accept message to all Paxos nodes: {}", message);
        for (int serverId : Config.getServerIdsInClusterExcept(nodeId)) {
            stubManager.getPaxosAsyncStub(serverId).accept(message.payload(), responseObserver);
        }
    }

    public void broadcastCommit(ServerMessage<CommitMessage> message) {
        ensureActive();
        logger.info("Broadcasting commit message to all Paxos nodes: {}", message);
        for (int serverId : Config.getServerIdsInClusterExcept(nodeId)) {
            stubManager.getPaxosStub(serverId).commit(message.payload());
        }
    }

    public void broadcastCheckpoint(ServerMessage<CheckpointMessage> message) {
        ensureActive();
        logger.info("Broadcasting checkpoint message to all Paxos nodes: {}", message);
        for (int serverId : Config.getServerIdsInClusterExcept(nodeId)) {
            stubManager.getPaxosStub(serverId).checkpoint(message.payload());
        }
    }

    public void broadcastCheckpointRequest(ServerMessage<CheckpointRequest> message, StreamObserver<CheckpointMessage> responseObserver) {
        ensureActive();
        logger.info("Broadcasting checkpoint request for seq {}", message.payload().getSequenceNumber());
        for (int serverId : Config.getServerIdsInClusterExcept(nodeId)) {
            stubManager.getPaxosAsyncStub(serverId).requestCheckpoint(message.payload(), responseObserver);
        }
    }
}
