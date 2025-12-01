package org.example.messaging;

import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.config.Config;
import org.example.state.Ballot;

import java.util.concurrent.ExecutorService;

public class PaxosMessageSender extends MessageSender {

    private static final Logger logger = LogManager.getLogger(PaxosMessageSender.class);

    public PaxosMessageSender(int nodeId, ExecutorService networkExecutor) {
        super(nodeId, networkExecutor);
    }

    public void forwardClientRequest(int targetNodeId, ServerMessage<ClientRequest> message) {
        logger.info("Forwarding client request to Paxos node {} : {}", targetNodeId, message);
        stubManager.getClientStub(targetNodeId).request(message.payload());
    }

    public void broadcastPrepare(ServerMessage<PrepareMessage> message, StreamObserver<PromiseMessage> responseObserver) {
        logger.info("Broadcasting prepare message to all Paxos nodes: {}", message);
        for (int serverId : Config.getServerIdsInClusterExcept(nodeId)) {
            stubManager.getPaxosAsyncStub(serverId).prepare(message.payload(), responseObserver);
        }
    }

    public void broadcastNewView(ServerMessage<NewViewMessage> message, StreamObserver<AcceptedMessage> responseObserver) {
        logger.info("Broadcasting new view message to all Paxos nodes: {}", message);
        for (int serverId : Config.getServerIdsInClusterExcept(nodeId)) {
            stubManager.getPaxosAsyncStub(serverId).newView(message.payload(), responseObserver);
        }
    }

    public void broadcastAccept(ServerMessage<AcceptMessage> message, StreamObserver<AcceptedMessage> responseObserver) {
        logger.info("Broadcasting accept message to all Paxos nodes: {}", message);
        for (int serverId : Config.getServerIdsInClusterExcept(nodeId)) {
            stubManager.getPaxosAsyncStub(serverId).accept(message.payload(), responseObserver);
        }
    }

    public void broadcastCommit(ServerMessage<CommitMessage> message) {
        logger.info("Broadcasting commit message to all Paxos nodes: {}", message);
        for (int serverId : Config.getServerIdsInClusterExcept(nodeId)) {
            stubManager.getPaxosStub(serverId).commit(message.payload());
        }
    }
}
