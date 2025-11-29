package org.example.messaging;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.config.Config;
import org.example.state.Ballot;

import java.util.concurrent.ExecutorService;

public class PaxosMessageSender extends MessageSender {

    private static final Logger logger = LogManager.getLogger(PaxosMessageSender.class);
    private final ExecutorService messageExecutor;

    public PaxosMessageSender(int nodeId, ExecutorService networkExecutor, ExecutorService messageExecutor) {
        super(nodeId, networkExecutor);
        this.messageExecutor = messageExecutor;
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

    public void broadcastNewView(NewViewMessage message, StreamObserver<AcceptedMessage> responseObserver) {
        logger.info("Broadcasting new view message to all Paxos nodes: for ballot {} with count {}", new Ballot(message.getBallot()), message.getAcceptLogCount());
        for (int serverId : Config.getServerIdsInClusterExcept(nodeId)) {
            stubManager.getPaxosAsyncStub(serverId).newView(message, responseObserver);
        }
    }

}
