package org.example.tpc.handlers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.ClientRequest;
import org.example.TPCPreparedMessage;
import org.example.messaging.ServerMessage;
import org.example.tpc.ClientRequestTracker;
import org.example.tpc.TPCTimer;

import java.util.function.Consumer;

public class PreparedHandler {
    private static final Logger logger = LogManager.getLogger(PreparedHandler.class);

    private final TPCTimer tpcTimer;
    private final ClientRequestTracker clientRequestTracker;
    private final Consumer<String> onPrepared;

    public PreparedHandler(TPCTimer tpcTimer, ClientRequestTracker clientRequestTracker,
                           Consumer<String> onPrepared) {
        this.tpcTimer = tpcTimer;
        this.clientRequestTracker = clientRequestTracker;
        this.onPrepared = onPrepared;
    }

    public void handle(ServerMessage<TPCPreparedMessage> prepared) {
        String requestId = prepared.payload().getRequestId();
        if (clientRequestTracker.markPrepared(requestId)) {
            tpcTimer.stop(requestId);
            if (clientRequestTracker.isConsensusCompleted(requestId)) {
                onPrepared.accept(requestId);
            }
        }
    }
}
