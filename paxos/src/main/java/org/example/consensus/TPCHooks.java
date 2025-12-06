package org.example.consensus;

import org.example.*;
import org.example.messaging.ServerMessage;

public interface TPCHooks {

    /**
     * Called whenever the local node's role changes to leader
     * - notifying TPC server that it can start processing client requests.
     */
    void onRoleChangeToLeader();

    /**
     * Called whenever the local node's role changes to backup
     * - notifying TPC server that it needs to stop retries and timers
     */
    void onRoleChangeToBackup();

    /**
     * Called when a new client request is received from leader in an accept, new view or commit message
     * - notifying TPC server that it should add it to the client request tracker.
     */
    void onNewClientRequest(ServerMessage<ClientRequest> request);

    /**
     * Called when triggering a Paxos new view
     * - notifying TPC server that it needs to resend TPC prepare / commit to participants.
     */
    void onPaxosNewView(ServerMessage<NewViewMessage> newViewMessage);

    /**
     * Called when a client request has been committed at the given sequence number.
     * The implementation is responsible for applying it to the state machine,
     * replying to clients, etc.
     */
    void onPaxosCommit(ServerMessage<CommitMessage> commitMessage);

    /**
     * Apply a checkpoint snapshot to the state machine.
     *
     * @param seqNum   the sequence number of the checkpoint
     * @param snapshot the checkpoint snapshot string
     */
    void applyCheckpoint(long seqNum, String snapshot);
}
