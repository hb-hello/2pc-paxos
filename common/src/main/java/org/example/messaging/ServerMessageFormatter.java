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
        } else if (msg instanceof AcceptMessage m) {
            return formatAccept(m);
        } else if (msg instanceof CommitMessage m) {
            return formatCommit(m);
        } else if (msg instanceof ClientRequest m) {
            return formatClientRequest(m);
        } else if (msg instanceof ClientReply m) {
            return formatClientReply(m);
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
                ", acceptLogSize=" + m.getAcceptLogCount() +
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
        return "ClientReply{clientId=" + m.getClientId() +
                ", sender=" + m.getSenderId() +
                ", ts=" + m.getTimestamp() +
                ", " + result +
                "}";
    }
}
