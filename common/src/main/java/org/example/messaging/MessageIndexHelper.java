package org.example.messaging;

import com.google.protobuf.MessageLite;
import org.example.*;

public final class MessageIndexHelper {

    private MessageIndexHelper() {}

    public static String computeIndex(MessageLite msg) {
        if (msg instanceof ClientRequest cr) {
            return clientRequestIndex(cr);
        }
        if (msg instanceof AcceptMessage am) {
            return paxosIndex(
                    am.getBallot().getInstance(),
                    am.getBallot().getSenderId(),
                    am.getSequenceNumber(),
                    am.getPhase().name()
            );
        }
        if (msg instanceof AcceptedMessage am) {
            return paxosIndex(
                    am.getBallot().getInstance(),
                    am.getBallot().getSenderId(),
                    am.getSequenceNumber(),
                    am.getPhase().name()
            );
        }
        if (msg instanceof CommitMessage cm) {
            return paxosIndex(
                    cm.getBallot().getInstance(),
                    cm.getBallot().getSenderId(),
                    cm.getSequenceNumber(),
                    cm.getPhase().name()
            );
        }
        if (msg instanceof PrepareMessage pm) {
            return pm.getBallot().getInstance() + ":" + pm.getBallot().getSenderId();
        }
        if (msg instanceof PromiseMessage pm) {
            return pm.getBallot().getInstance() + ":" + pm.getSenderId();
        }

        // Future tpc.proto messages go here:
        // if (msg instanceof TpcPrepareMessage t) { ... }

        // Generic fallback: still stable per-process for debugging, but not for protocol logic.
        return msg.getClass().getSimpleName() + ":" + msg.hashCode();
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
        return cr.getClientId() + ":" + cr.getTimestamp();
    }

    private static String paxosIndex(long instance, int senderId,
                                     long seq, String phaseName) {
        return instance + ":" + senderId + ":" + seq + ":" + phaseName;
    }
}
