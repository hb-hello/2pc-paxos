package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.messaging.ServerMessage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tracks client requests and their replies, keyed by ServerMessage.messageId.
 * Thread-safe and scalable for thousands of concurrent requests.
 */
public final class ClientRequestTracker {
    private static final Logger logger = LogManager.getLogger(ClientRequestTracker.class);

    /**
     * Per-client-request state.
     * One instance per unique messageId.
     */
    private static final class Entry {
        private final ServerMessage<ClientRequest> request;   // may be null if reply arrives first
        private volatile ServerMessage<ClientReply> reply;  // set once when reply is available

        private Entry(ServerMessage<ClientRequest> request) {
            this.request = request;
        }

        void setReply(ServerMessage<ClientReply> reply) {
            this.reply = reply;
        }

        ServerMessage<ClientReply> getReply() {
            return reply;
        }

        ServerMessage<ClientRequest> getRequest() {
            return request;
        }
    }

    // messageId -> Entry
    private final ConcurrentMap<String, Entry> requests = new ConcurrentHashMap<>();

    public boolean hasRequest(ServerMessage<ClientRequest> message) {
        String key = message.getMessageId();  // uses ServerMessage's id helper [attached_file:1]
        return requests.containsKey(key);
    }

    public void addRequest(ServerMessage<ClientRequest> message) {
        String key = message.getMessageId();
        requests.computeIfAbsent(key, k -> new Entry(message));
        logger.info("Added request with messageId={}", key);
    }

    public void addReply(ServerMessage<ClientReply> replyMessage) {
        String key = replyMessage.getMessageId();
        requests.compute(key, (k, existing) -> {
            if (existing == null) {
                existing = new Entry(null);
            }
            existing.setReply(replyMessage);
            return existing;
        });
        logger.info("Added reply {} for messageId={}", replyMessage, key);
    }

    /**
     * Returns the reply ServerMessage if present, otherwise null.
     */
    public ServerMessage<ClientReply> getReply(ServerMessage<ClientRequest> requestMessage) {
        String key = requestMessage.getMessageId();
        Entry e = requests.get(key);
        return e != null ? e.getReply() : null;
    }

    public void remove(ServerMessage<ClientRequest> message) {
        String key = message.getMessageId();
        requests.remove(key);
    }
}
