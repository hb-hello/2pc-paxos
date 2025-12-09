package org.example.messaging;

import com.google.protobuf.MessageLite;
import org.example.*;

public final class MessageIndexHelper {

    private MessageIndexHelper() {}

    public static String computeIndex(MessageLite msg) {
        if (msg instanceof ClientRequest cr) {
            return clientRequestIndex(cr);
        }
        if (msg instanceof ClientReply cr) {
            return cr.getRequestId();
        }
        String messageIndex = String.valueOf(msg.hashCode());
        if (msg instanceof AcceptMessage am) {
            messageIndex = paxosIndex(
                    am.getBallot().getInstance(),
                    am.getBallot().getSenderId(),
                    am.getSequenceNumber(),
                    am.getPhase().name()
            );
        }
        if (msg instanceof AcceptedMessage am) {
            messageIndex = paxosIndex(
                    am.getBallot().getInstance(),
                    am.getBallot().getSenderId(),
                    am.getSequenceNumber(),
                    am.getPhase().name()
            );
        }
        if (msg instanceof CommitMessage cm) {
            messageIndex = paxosIndex(
                    cm.getBallot().getInstance(),
                    cm.getBallot().getSenderId(),
                    cm.getSequenceNumber(),
                    cm.getPhase().name()
            );
        }
        if (msg instanceof PrepareMessage pm) {
            messageIndex = pm.getBallot().getInstance() + ":" + pm.getBallot().getSenderId();
        }
        if (msg instanceof PromiseMessage pm) {
            messageIndex = pm.getBallot().getInstance() + ":" + pm.getBallot().getSenderId();
        }
        if (msg instanceof NewViewMessage pm) {
            messageIndex = pm.getBallot().getInstance() + ":" + pm.getBallot().getSenderId();
        }
        if (msg instanceof CheckpointMessage cm) {
            messageIndex = String.valueOf(cm.getSequenceNumber());
        }
        if (msg instanceof TPCPrepareMessage m) {
            messageIndex = clientRequestIndex(m.getClientRequest());
        }
        if (msg instanceof TPCPreparedMessage m) {
            messageIndex = m.getRequestId();
        }
        if (msg instanceof TPCCommitMessage m) {
            messageIndex = m.getRequestId();
        }
        if (msg instanceof TPCAbortMessage m) {
            messageIndex = m.getRequestId();
        }

        // Future tpc.proto messages go here:
        // if (msg instanceof TpcPrepareMessage t) { ... }

        // Generic fallback: still stable per-process for debugging, but not for protocol logic.
        return msg.getClass().getSimpleName() + ":" + messageIndex;
    }

    public static int extractSenderId(MessageLite msg) {
        if (msg instanceof PromiseMessage pm) {
            return pm.getSenderId();
        }
        if (msg instanceof AcceptedMessage am) {
            return am.getSenderId();
        }
        return -1;
    }

    private static String clientRequestIndex(ClientRequest cr) {
        return cr.getRequestId();
    }

    private static String paxosIndex(long instance, int senderId,
                                     long seq, String phaseName) {
        return instance + ":" + senderId + ":" + seq + ":" + phaseName;
    }
}
