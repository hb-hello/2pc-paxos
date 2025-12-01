package org.example.consensus.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.CommitMessage;
import org.example.messaging.ServerMessage;
import org.example.state.Ballot;
import org.example.state.PaxosState;

import java.util.function.Consumer;

public class CommitHandler {
    private static final Logger logger = LogManager.getLogger(CommitHandler.class);

    private final PaxosState state;
    private Consumer<ServerMessage<CommitMessage>> onCommit;

    public CommitHandler(PaxosState state, Consumer<ServerMessage<CommitMessage>> onCommit) {
        this.state = state;
        this.onCommit = onCommit;
    }

    public void handle(ServerMessage<CommitMessage> commit) {
        Ballot commitBallot = new Ballot(commit.payload().getBallot());

        if (state.checkBallotAndTransitionToBackup(commitBallot)) {
            onCommit.accept(commit);
        } else {
            logger.info("Commit request rejected for ballot number: {}", commitBallot);
        }
    }
}
