package org.example.tpc;

import org.example.ClientReply;
import org.example.ClientRequest;
import org.example.messaging.ServerMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tracks client requests and (optionally) their replies to handle duplicates.
 */
public class ClientRequestTracker {

    /**
     * One entry per logical client request.
     */
    private static final class Entry {
        private final ServerMessage<ClientRequest> request;
        private volatile ServerMessage<ClientReply> reply;

        // Has this request been accepted by Paxos / 2PC?
        private volatile boolean accepted = false;

        private Entry(ServerMessage<ClientRequest> request) {
            this.request = request;
        }

        public ServerMessage<ClientRequest> getRequest() {
            return request;
        }

        public ServerMessage<ClientReply> getReply() {
            return reply;
        }

        public void setReply(ServerMessage<ClientReply> reply) {
            this.reply = reply;
        }

        public boolean isAccepted() {
            return accepted;
        }

        public void markAccepted() {
            this.accepted = true;
        }
    }

    // All known requests, keyed by messageId (clientId+timestamp via ServerMessage). [attached_file:1]
    private final ConcurrentMap<String, Entry> entries = new ConcurrentHashMap<>();

    // Only ids for requests that are not yet marked accepted.
    private final Set<String> pendingIds = ConcurrentHashMap.newKeySet();

    private String key(ServerMessage<ClientRequest> request) {
        return request.getMessageId();
    }

    public boolean hasRequest(ServerMessage<ClientRequest> request) {
        return entries.containsKey(key(request));
    }

    public void addRequest(ServerMessage<ClientRequest> request) {
        String id = key(request);
        Entry newEntry = new Entry(request);
        Entry existing = entries.putIfAbsent(id, newEntry);
        // Only mark as pending if this is the first time we see this id.
        if (existing == null) {
            pendingIds.add(id);
        }
    }

    public void storeReply(ServerMessage<ClientRequest> request,
                           ServerMessage<ClientReply> reply) {
        Entry e = entries.get(key(request));
        if (e != null) {
            e.setReply(reply);
        }
    }

    public ServerMessage<ClientReply> getReply(ServerMessage<ClientRequest> request) {
        Entry e = entries.get(key(request));
        return e != null ? e.getReply() : null;
    }

    // ---------- Accepted flag API ----------

    public void markAccepted(ServerMessage<ClientRequest> request) {
        String id = key(request);
        Entry e = entries.get(id);
        if (e != null) {
            e.markAccepted();
            pendingIds.remove(id);
        }
    }

    public void markAccepted(String messageId) {
        Entry e = entries.get(messageId);
        if (e != null) {
            e.markAccepted();
            pendingIds.remove(messageId);
        }
    }

    public boolean isAccepted(ServerMessage<ClientRequest> request) {
        Entry e = entries.get(key(request));
        return e != null && e.isAccepted();
    }

    /**
     * Returns a snapshot of all client requests that have not yet been marked accepted.
     * Iterates only over the pendingIds set, not all entries.
     */
    public Collection<ServerMessage<ClientRequest>> getPendingClientRequests() {
        Collection<ServerMessage<ClientRequest>> result = new ArrayList<>();
        for (String id : pendingIds) {
            Entry e = entries.get(id);
            if (e != null && !e.isAccepted()) {
                result.add(e.getRequest());
            } else {
                // Cleanup: if entry missing or already accepted, remove from pendingIds.
                pendingIds.remove(id);
            }
        }
        return result;
    }
}
