package org.example.tpc;

import org.example.ClientReply;
import org.example.ClientRequest;
import org.example.Operation;
import org.example.Phase;
import org.example.messaging.ServerMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
        private final ExecutionMode mode;
        private final int otherClusterIndex;

        // Has this request been accepted by Paxos / 2PC?
        private final AtomicBoolean accepted = new AtomicBoolean(false);

        // which 2PC the phase the request is in
        private final AtomicReference<Phase> phase = new AtomicReference<>(null);

        // has ack been received from participant for the corresponding commit / abort
        private final AtomicBoolean ackReceived = new AtomicBoolean(false);

        // has intra-cluster consensus been completed for this client request once
        private final AtomicBoolean consensusCompleted = new AtomicBoolean(false);

        private Entry(ServerMessage<ClientRequest> request, ExecutionMode mode, int otherClusterIndex) {
            this.request = request;
            this.mode = mode;
            this.otherClusterIndex = otherClusterIndex;
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
            return accepted.get();
        }

        public void markAccepted() {
            this.accepted.set(true);
        }

        public ExecutionMode getMode() {
            return mode;
        }

        public int getOtherClusterIndex() {
            return otherClusterIndex;
        }

        public Phase getPhase() {
            return phase.get();
        }

        public void setPhase(Phase phase) {
            this.phase.set(phase);
        }

        public boolean isAckReceived() {
            return ackReceived.get();
        }

        public void setAckReceived() {
            this.ackReceived.set(true);
        }

        public boolean isConsensusCompleted() {
            return consensusCompleted.get();
        }

        public void setConsensusCompleted(boolean consensusCompleted) {
            this.consensusCompleted.set(consensusCompleted);
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

    public boolean hasRequest(String requestId) {
        return entries.containsKey(requestId);
    }

    public void addRequest(ServerMessage<ClientRequest> request, ExecutionMode mode, int otherClusterIndex) {
        String id = key(request);
        Entry newEntry = new Entry(request, mode, otherClusterIndex);
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
        return getReply(key(request));
    }

    public ServerMessage<ClientReply> getReply(String requestId) {
        Entry e = entries.get(requestId);
        return e != null ? e.getReply() : null;
    }

    public Operation getOperation(String requestId) {
        Entry e = entries.get(requestId);
        return e != null ? e.getRequest().payload().getOperation() : null;
    }

    public ExecutionMode getExecutionMode(ServerMessage<ClientRequest> request) {
        return getExecutionMode(key(request));
    }

    public ExecutionMode getExecutionMode(String requestId) {
        Entry e = entries.get(requestId);
        return e != null ? e.getMode() : null;
    }

    public int getOtherClusterIndex(ServerMessage<ClientRequest> request) {
        return getOtherClusterIndex(key(request));
    }

    public int getOtherClusterIndex(String requestId) {
        Entry e = entries.get(requestId);
        return e != null ? e.getOtherClusterIndex() : -1;
    }

    // ---------- Accepted flag API ----------

    public void markAccepted(ServerMessage<ClientRequest> request) {
        markAccepted(key(request));
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

    // ---------- Ack received flag API ----------

    public void markAckReceived(ServerMessage<ClientRequest> request) {
        markAckReceived(key(request));
    }

    public void markAckReceived(String requestId) {
        Entry e = entries.get(requestId);
        if (e != null) {
            e.setAckReceived();
        }
    }

    public boolean isAckReceived(ServerMessage<ClientRequest> request) {
        return isAckReceived(key(request));
    }

    public boolean isAckReceived(String requestId) {
        Entry e = entries.get(requestId);
        return e != null && e.isAckReceived();
    }

    // ---------- Consensus completed flag API ----------

    public void markConsensusCompleted(ServerMessage<ClientRequest> request) {
        markConsensusCompleted(key(request));
    }

    public void markConsensusCompleted(String requestId) {
        Entry e = entries.get(requestId);
        if (e != null) {
            e.setConsensusCompleted(true);
        }
    }

    public boolean isConsensusCompleted(ServerMessage<ClientRequest> request) {
        return isConsensusCompleted(key(request));
    }

    public boolean isConsensusCompleted(String requestId) {
        Entry e = entries.get(requestId);
        return e != null && e.isConsensusCompleted();
    }

    // ---------- Phase tracking API ----------

    private boolean tryTransitionPhase(Entry entry, Phase targetPhase) {
        if (entry == null) {
            return false;
        }
        Phase current = entry.getPhase();
        boolean allowed =
                (current == null && (targetPhase == Phase.PREPARE || targetPhase == Phase.INTRA_SHARD)) ||
                        (current == Phase.PREPARE && (targetPhase == Phase.COMMIT || targetPhase == Phase.ABORT));
        if (!allowed) {
            return false;
        }
        entry.setPhase(targetPhase);
        return true;
    }

    public boolean markPrepared(ServerMessage<ClientRequest> request) {
        return tryTransitionPhase(entries.get(key(request)), Phase.PREPARE);
    }

    // only be called when we receive prepared from participant
    public boolean markPrepared(String requestId) {
        return tryTransitionPhase(entries.get(requestId), Phase.PREPARE);
    }

    // only be called when we receive commit from leader or send commit to participant
    public boolean markCommitted(ServerMessage<ClientRequest> request) {
        return markCommitted(key(request));
    }

    public boolean markCommitted(String requestId) {
        return tryTransitionPhase(entries.get(requestId), Phase.COMMIT);
    }

    // only be called when we receive abort from leader or send abort to participant
    public boolean markAborted(ServerMessage<ClientRequest> request) {
        return tryTransitionPhase(entries.get(key(request)), Phase.ABORT);
    }

    public boolean markIntraShard(ServerMessage<ClientRequest> request) {
        return tryTransitionPhase(entries.get(key(request)), Phase.INTRA_SHARD);
    }

    public boolean isPrepared(ServerMessage<ClientRequest> request) {
        Entry e = entries.get(key(request));
        return e != null && e.getPhase() == Phase.PREPARE;
    }

    public boolean isCommitted(ServerMessage<ClientRequest> request) {
       return isCommitted(key(request));
    }

    public boolean isCommitted(String requestId) {
        Entry e = entries.get(requestId);
        return e != null && e.getPhase() == Phase.COMMIT;
    }

    public boolean isAborted(ServerMessage<ClientRequest> request) {
        Entry e = entries.get(key(request));
        return e != null && e.getPhase() == Phase.ABORT;
    }

    public boolean isIntraShard(ServerMessage<ClientRequest> request) {
        Entry e = entries.get(key(request));
        return e != null && e.getPhase() == Phase.INTRA_SHARD;
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

    public void removeReply(ServerMessage<ClientRequest> request) {
        Entry e = entries.get(request.getMessageId());
        if (e != null) {
            e.setReply(null);
        }
    }

}
