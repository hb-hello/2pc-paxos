package org.example.consensus;

import org.example.CommitMessage;
import org.example.messaging.ServerMessage;

public interface TPCHooks {

    /**
     * Called whenever the local node's role changes, including
     * transitions to and from LEADER.
     */
//    void onRoleChange(Role newRole, int leaderId);

    /**
     * Called when a client request has been committed at the given sequence number.
     * The implementation is responsible for applying it to the state machine,
     * replying to clients, etc.
     */
    void onPaxosCommit(ServerMessage<CommitMessage> commitMessage);
}
