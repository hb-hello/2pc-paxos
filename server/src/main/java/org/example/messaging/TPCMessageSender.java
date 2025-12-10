package org.example.messaging;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.config.Config;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class TPCMessageSender extends MessageSender {

    private static final Logger logger = LogManager.getLogger(TPCMessageSender.class);

    public TPCMessageSender(int nodeId, ExecutorService networkExecutor) {
        super(nodeId, networkExecutor);
    }

    public void sendPrepare(int targetNodeId, ServerMessage<TPCPrepareMessage> prepare, StreamObserver<TPCAckMessage> responseObserver) {
        ensureActive();
        logger.info("Sending TPC Prepare to node {} : {}", targetNodeId, prepare);
        stubManager.getTPCAsyncStub(targetNodeId).withDeadlineAfter(10, TimeUnit.MILLISECONDS).tPCPrepare(prepare.payload(), responseObserver);
    }

    public void broadcastPrepareToCluster(int targetNodeId, ServerMessage<TPCPrepareMessage> prepare) {
        ensureActive();
        logger.info("Broadcasting TPC Prepare to cluster of node {} : {}", targetNodeId, prepare);
        for (int nodeId : Config.getServerIdsInCluster(Config.getServerClusterIndex(targetNodeId))) {
            stubManager.getTPCStub(targetNodeId).tPCPrepare(prepare.payload());
        }
    }

    public void sendPrepared(int targetNodeId, ServerMessage<TPCPreparedMessage> prepared) {
        ensureActive();
        logger.info("Sending TPC Prepared to node {} : {}", targetNodeId, prepared);
        stubManager.getTPCStub(targetNodeId).tPCPrepared(prepared.payload());
    }

    public void sendCommit(int targetNodeId, ServerMessage<TPCCommitMessage> commit, StreamObserver<TPCAckMessage> responseObserver) {
        ensureActive();
        logger.info("Sending TPC Commit to node {} : {}", targetNodeId, commit);
        stubManager.getTPCAsyncStub(targetNodeId).withDeadlineAfter(Config.getServerTimeoutMillis(), TimeUnit.MILLISECONDS).tPCCommit(commit.payload(), responseObserver);
    }

    public void broadcastCommitToCluster(int targetNodeId, ServerMessage<TPCCommitMessage> commit, StreamObserver<TPCAckMessage> observer) {
        ensureActive();
        for (int nodeId : Config.getServerIdsInCluster(Config.getServerClusterIndex(targetNodeId))) {
            sendCommit(nodeId, commit, observer);
        }
    }

    public void sendAbort(int targetNodeId, ServerMessage<TPCAbortMessage> abort, StreamObserver<TPCAckMessage> responseObserver) {
        ensureActive();
        logger.info("Sending TPC Abort to node {} : {}", targetNodeId, abort);
        stubManager.getTPCAsyncStub(targetNodeId).withDeadlineAfter(Config.getServerTimeoutMillis(), TimeUnit.MILLISECONDS).tPCAbort(abort.payload(), responseObserver);
    }

    public void sendAbortWithoutResponse(int targetNodeId, ServerMessage<TPCAbortMessage> abort) {
        ensureActive();
        logger.info("Sending TPC Abort without response to node {} : {}", targetNodeId, abort);
        stubManager.getTPCStub(targetNodeId).tPCAbort(abort.payload());
    }

    public void broadcastAbortToCluster(int targetNodeId, ServerMessage<TPCAbortMessage> abort, StreamObserver<TPCAckMessage> observer) {
        ensureActive();
        for (int nodeId : Config.getServerIdsInCluster(Config.getServerClusterIndex(targetNodeId))) {
            sendAbort(nodeId, abort, observer);
        }
    }

    public void sendClientReply(ServerMessage<ClientReply> reply) {
        ensureActive();
        logger.info("Sending Client Reply : {}", reply);
        stubManager.getClientStub(0).reply(reply.payload());
    }

    public void broadcastLeaderElected(int serverId) {
        ensureActive();
        logger.info("Broadcasting Leader Elected to all servers");
        NewLeader newLeader = NewLeader.newBuilder()
                .setSenderId(serverId)
                .build();
        for (int targetNodeId : Config.getAllServerIdsExceptInCluster(Config.getServerClusterIndex(serverId))) {
            stubManager.getTPCStub(targetNodeId).leaderElected(newLeader);
        }
    }
}
