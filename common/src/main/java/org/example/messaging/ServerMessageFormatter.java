package org.example.messaging;

import com.google.protobuf.MessageLite;
import org.example.*;  // PrepareMessage, PromiseMessage, AcceptMessage, CommitMessage, ClientRequest, ClientReply, etc. [attached_file:1]

public final class ServerMessageFormatter {

    private ServerMessageFormatter() {}

    public static String format(MessageLite msg) {
        if (msg instanceof PrepareMessage m) {
            return formatPrepare(m);
        } else if (msg instanceof PromiseMessage m) {
            return formatPromise(m);
        } else if (msg instanceof NewViewMessage m) {
            return formatNewView(m);
        }
        else if (msg instanceof AcceptMessage m) {
            return formatAccept(m);
        } else if (msg instanceof AcceptedMessage m) {
            return formatAccepted(m);
        } else if (msg instanceof CommitMessage m) {
            return formatCommit(m);
        } else if (msg instanceof ClientRequest m) {
            return formatClientRequest(m);
        } else if (msg instanceof ClientReply m) {
            return formatClientReply(m);
        } else if (msg instanceof TPCPrepareMessage m) {
            return formatTPCPrepare(m);
        } else if (msg instanceof TPCPreparedMessage m) {
            return formatTPCPrepared(m);
        } else if (msg instanceof TPCCommitMessage m) {
            return formatTPCCommit(m);
        } else if (msg instanceof TPCAbortMessage m) {
            return formatTPCAbort(m);
        }
        // When tpc.proto exists, add:
        // else if (msg instanceof TpcPrepareMessage m) { ... }

        // Fallback: protobuf's own toString if you ever need it for debugging
        return msg.toString();
    }

    private static String formatPrepare(PrepareMessage m) {
        return "Prepare{instance=" + m.getBallot().getInstance() +
                ", sender=" + m.getBallot().getSenderId() +
                "}";
    }

    private static String formatPromise(PromiseMessage m) {
        return "Promise{instance=" + m.getBallot().getInstance() +
                ", sender=" + m.getSenderId() +
                ", acceptMessageLogCount=" + m.getAcceptLogCount() +
                ", commitMessageLogCount=" + m.getCommitLogCount() +
                "}";
    }

    private static String formatNewView(NewViewMessage m) {
        return "NewView{instance=" + m.getBallot().getInstance() +
                ", sender=" + m.getBallot().getSenderId() +
                ", acceptMessageLogCount=" + m.getAcceptLogCount() +
                ", commitMessageLogCount=" + m.getCommitLogCount() +
                "}";
    }

    private static String txIDFromRequest(ClientRequest req) {
        return req.getClientId() + ":" + req.getTimestamp();
    }

    private static String formatAccept(AcceptMessage m) {
        return "Accept{instance=" + m.getBallot().getInstance() +
                ", seq=" + m.getSequenceNumber() +
                ", phase=" + m.getPhase().name() +
                ", txId=" + txIDFromRequest(m.getRequest()) +
                "}";
    }

    private static String formatAccepted(AcceptedMessage m) {
        return "Accepted{instance=" + m.getBallot().getInstance() +
                ", seq=" + m.getSequenceNumber() +
                ", phase=" + m.getPhase().name() +
                ", sender=" + m.getSenderId() +
                "}";
    }

    private static String formatCommit(CommitMessage m) {
        return "Commit{instance=" + m.getBallot().getInstance() +
                ", seq=" + m.getSequenceNumber() +
                ", phase=" + m.getPhase().name() +
                ", txId=" + txIDFromRequest(m.getRequest()) +
                "}";
    }

    private static String formatClientRequest(ClientRequest m) {
        String opType = m.getOperation().hasTransfer() ? "TRANSFER" : "BALANCE";
        return "ClientRequest{clientId=" + m.getClientId() +
                ", ts=" + m.getTimestamp() +
                ", op=" + opType +
                ", requestId=" + m.getRequestId() +
                "}";
    }

    private static String formatClientReply(ClientReply m) {
        String result;
        if (m.getResult().hasSuccess()) {
            result = "success=" + m.getResult().getSuccess();
        } else if (m.getResult().hasBalance()) {
            result = "balance=" + m.getResult().getBalance();
        } else {
            result = "emptyResult";
        }
        return "ClientReply{requestId=" + m.getRequestId() +
                ", sender=" + m.getSenderId() +
                ", " + result +
                "}";
    }

    private static String formatTPCPrepare(TPCPrepareMessage m) {
        return "TPCPrepare{request=" + formatClientRequest(m.getClientRequest()) +
                ", sender=" + m.getSenderId() +
                "}";
    }

    private static String formatTPCPrepared(TPCPreparedMessage m) {
        return "TPCPrepared{requestId=" + m.getRequestId() +
                ", sender=" + m.getSenderId() +
                "}";
    }

    private static String formatTPCCommit(TPCCommitMessage m) {
        return "TPCCommit{requestId=" + m.getRequestId() +
                ", sender=" + m.getSenderId() +
                "}";
    }

    private static String formatTPCAbort(TPCAbortMessage m) {
        return "TPCAbort{requestId=" + m.getRequestId() +
                ", sender=" + m.getSenderId() +
                "}";
    }
}
