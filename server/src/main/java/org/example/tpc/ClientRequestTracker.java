package org.example.tpc;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.ClientReply;
import org.example.ClientRequest;
import org.example.Operation;
import org.example.Phase;
import org.example.TPCAckMessage;
import org.example.messaging.ServerMessage;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks client requests and (optionally) their replies to handle duplicates.
 */
public class ClientRequestTracker {

    private static final Logger logger = LogManager.getLogger(ClientRequestTracker.class);

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

        // has intra-cluster consensus been completed for this client request for phase 1 of 2PC, i.e., PREPARE phase
        private final AtomicBoolean consensusCompletedPhase1 = new AtomicBoolean(false);

        // has intra-cluster consensus been completed for this client request for phase 2 of 2PC, i.e., COMMIT / ABORT phase
        private final AtomicBoolean consensusCompletedPhase2 = new AtomicBoolean(false);
        // observer to respond to participant ACKs for this request (might be set/cleared dynamically)
        private final AtomicReference<StreamObserver<TPCAckMessage>> ackResponseObserverRef = new AtomicReference<>();

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

        public boolean getConsensusCompletedPhase1() {
            return consensusCompletedPhase1.get();
        }

        public void setConsensusCompletedPhase1(boolean consensusCompletedPhase1) {
            this.consensusCompletedPhase1.set(consensusCompletedPhase1);
        }

        public boolean getConsensusCompletedPhase2() {
            return consensusCompletedPhase2.get();
        }

        public void setConsensusCompletedPhase2(boolean consensusCompletedPhase2) {
            this.consensusCompletedPhase2.set(consensusCompletedPhase2);
        }

        // AckResponseObserver API on Entry
        public StreamObserver<TPCAckMessage> getAckResponseObserver() {
            return ackResponseObserverRef.get();
        }

        public void setAckResponseObserver(StreamObserver<TPCAckMessage> observer) {
            ackResponseObserverRef.set(observer);
        }

        public void removeAckResponseObserver() {
            ackResponseObserverRef.set(null);
        }

        /**
         * Atomically fetch-and-clear the observer and send the ack message (onNext + onCompleted).
         * Safe to call concurrently; only one caller will get the observer.
         */
        public void sendAckResponse(TPCAckMessage msg) {
            StreamObserver<TPCAckMessage> obs = ackResponseObserverRef.getAndSet(null);
            if (obs == null) return;
            try {
                obs.onNext(msg);
                obs.onCompleted();
            } catch (Throwable t) {
                logger.error("Error while sending TPCAckMessage to observer for request {}", request.getMessageId(), t);
                try {
                    obs.onError(t);
                } catch (Throwable ignore) {
                }
            }
        }
    }

    // All known requests, keyed by messageId (clientId+timestamp via ServerMessage)
    private final ConcurrentMap<String, Entry> entries = new ConcurrentHashMap<>();

    // Only ids for requests that are not yet marked accepted.
    private final Set<String> pendingIds = ConcurrentHashMap.newKeySet();

    // Map otherClusterIndex -> set of requestIds where the request's mode == SENDER
    private final ConcurrentMap<Integer, Set<String>> senderRequestIdsByOtherCluster = new ConcurrentHashMap<>();

    private String key(ServerMessage<ClientRequest> request) {
        return request.getMessageId();
    }

    public boolean hasRequest(ServerMessage<ClientRequest> request) {
        return entries.containsKey(key(request));
    }

    public boolean hasRequest(String requestId) {
        return entries.containsKey(requestId);
    }

    public ServerMessage<ClientRequest> getRequest(String requestId) {
        return entries.get(requestId).getRequest();
    }

    public void addRequest(ServerMessage<ClientRequest> request, ExecutionMode mode, int otherClusterIndex) {
        Entry newEntry = new Entry(request, mode, otherClusterIndex);
        String id = key(request);
        Entry existing = entries.putIfAbsent(id, newEntry);
        // Only mark as pending if this is the first time we see this id.
        if (existing == null) {
            pendingIds.add(id);
        }
        // Track SENDER-mode request ids by otherClusterIndex (idempotent)
        if (mode == ExecutionMode.SENDER) {
            senderRequestIdsByOtherCluster.computeIfAbsent(otherClusterIndex, k -> ConcurrentHashMap.newKeySet()).add(id);
        }
    }

    public void addAcceptedRequest(ServerMessage<ClientRequest> request, ExecutionMode mode, int otherClusterIndex) {
        Entry newEntry = new Entry(request, mode, otherClusterIndex);
        String id = key(request);
        entries.putIfAbsent(id, newEntry);
        markAccepted(request);
        // Also ensure SENDER-mode requests are tracked here as well
        if (mode == ExecutionMode.SENDER) {
            senderRequestIdsByOtherCluster.computeIfAbsent(otherClusterIndex, k -> ConcurrentHashMap.newKeySet()).add(id);
        }
    }

    public void setReply(ServerMessage<ClientRequest> request,
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
        return isAccepted(key(request));
    }

    public boolean isAccepted(String requestId) {
        Entry e = entries.get(requestId);
        return e != null && e.isAccepted();
    }

    public boolean compareAndMarkAccepted(ServerMessage<ClientRequest> request) {
        String id = key(request);
        Entry e = entries.get(id);
        if (e != null && !e.isAccepted()) {
            e.markAccepted();
            pendingIds.remove(id);
            return true;
        }
        return false;
    }

    // ---------- Ack received flag API ----------

    public void markAckReceived(ServerMessage<ClientRequest> request) {
        markAckReceived(key(request));
    }

    public void markAckReceived(String requestId) {
        Entry e = entries.get(requestId);
        if (e != null) {
            e.setAckReceived();
            // Remove from senderRequestIdsByOtherCluster if present
            int otherCluster = e.getOtherClusterIndex();
            senderRequestIdsByOtherCluster.computeIfPresent(otherCluster, (k, set) -> {
                set.remove(requestId);
                // remove the bucket if empty
                return set.isEmpty() ? null : set;
            });
            logger.info("Marked ACK received for request {}, removed from waiting list for otherClusterIndex {}",
                    requestId, otherCluster);
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

    public void markConsensusCompletedPhase1(ServerMessage<ClientRequest> request) {
        markConsensusCompletedPhase1(key(request));
    }

    public void markConsensusCompletedPhase1(String requestId) {
        Entry e = entries.get(requestId);
        if (e != null) {
            e.setConsensusCompletedPhase1(true);
        }
    }

    public boolean isConsensusCompletedPhase1(ServerMessage<ClientRequest> request) {
        return isConsensusCompletedPhase1(key(request));
    }

    public boolean isConsensusCompletedPhase1(String requestId) {
        Entry e = entries.get(requestId);
        return e != null && e.getConsensusCompletedPhase1();
    }

    public void markConsensusCompletedPhase2(ServerMessage<ClientRequest> request) {
        markConsensusCompletedPhase2(key(request));
    }

    public void markConsensusCompletedPhase2(String requestId) {
        Entry e = entries.get(requestId);
        if (e != null) {
            e.setConsensusCompletedPhase2(true);
        }
    }

    public boolean isConsensusCompletedPhase2(ServerMessage<ClientRequest> request) {
        return isConsensusCompletedPhase2(key(request));
    }

    public boolean isConsensusCompletedPhase2(String requestId) {
        Entry e = entries.get(requestId);
        return e != null && e.getConsensusCompletedPhase2();
    }

    // ---------- Phase tracking API ----------

    private boolean tryTransitionPhase(Entry entry, Phase targetPhase) {
        if (entry == null) {
            return false;
        }
        Phase current = entry.getPhase();
        boolean allowed =
                (current == null) ||
                        (current == Phase.PREPARE && (targetPhase == Phase.COMMIT || targetPhase == Phase.ABORT));
        if (!allowed) {
            logger.warn("Invalid phase transition from {} to {} for request {}",
                    current, targetPhase, entry.getRequest().getMessageId());
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
        return markAborted(key(request));
    }

    public boolean markAborted(String requestId) {
        return tryTransitionPhase(entries.get(requestId), Phase.ABORT);
    }

    public boolean markIntraShard(ServerMessage<ClientRequest> request) {
        return tryTransitionPhase(entries.get(key(request)), Phase.INTRA_SHARD);
    }

    public boolean isPrepared(ServerMessage<ClientRequest> request) {
        return isPrepared(key(request));
    }

    public boolean isPrepared(String requestId) {
        Entry e = entries.get(requestId);
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
        return isAborted(key(request));
    }

    public boolean isAborted(String requestId) {
        Entry e = entries.get(requestId);
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

    // ---------- AckResponseObserver API ----------

    public void setAckResponseObserver(ServerMessage<ClientRequest> request, StreamObserver<TPCAckMessage> observer) {
        setAckResponseObserver(request.getMessageId(), observer);
    }

    public void setAckResponseObserver(String requestId, StreamObserver<TPCAckMessage> observer) {
        Entry e = entries.get(requestId);
        if (e != null) {
            e.setAckResponseObserver(observer);
        }
    }

    public StreamObserver<TPCAckMessage> getAckResponseObserver(ServerMessage<ClientRequest> request) {
        Entry e = entries.get(key(request));
        return e != null ? e.getAckResponseObserver() : null;
    }

    public void removeAckResponseObserver(ServerMessage<ClientRequest> request) {
        Entry e = entries.get(key(request));
        if (e != null) {
            e.removeAckResponseObserver();
        }
    }

    /**
     * Helper to atomically send an ack response to the observer registered for the given requestId.
     * If an observer is registered, it will be consumed (cleared) and the message sent.
     *
     * @param requestId client request id
     */
    public void sendAckResponse(String requestId) {
        Entry e = entries.get(requestId);
        if (e != null) {
            TPCAckMessage msg = TPCAckMessage.newBuilder().build();
            e.sendAckResponse(msg);
        }
    }

    public String printTrackedRequests() {
        StringBuilder sb = new StringBuilder();
        for (Entry entry : entries.values()) {
            sb.append("Request ID: ").append(entry.getRequest().getMessageId())
                    .append(", Accepted: ").append(entry.isAccepted())
                    .append(", Phase: ").append(entry.getPhase())
                    .append(", AckReceived: ").append(entry.isAckReceived())
                    .append(", ConsensusCompletedPhase1: ").append(entry.getConsensusCompletedPhase1())
                    .append(", ConsensusCompletedPhase2: ").append(entry.getConsensusCompletedPhase2())
                    .append("\n");
        }
        return sb.toString();
    }

    public void reset() {
        entries.clear();
        pendingIds.clear();
        senderRequestIdsByOtherCluster.clear();
    }

    /**
     * Return an unmodifiable snapshot of request IDs for the given otherClusterIndex.
     */
    public Set<String> getRequestsForOtherCluster(int otherClusterIndex) {
        Set<String> s = senderRequestIdsByOtherCluster.get(otherClusterIndex);
        if (s == null) return Collections.emptySet();
        return Set.copyOf(s);
    }

    /**
     * Return a snapshot map of all otherClusterIndex -> requestId sets (unmodifiable).
     */
    public Map<Integer, Set<String>> getAllSenderRequestsByOtherCluster() {
        Map<Integer, Set<String>> copy = new HashMap<>();
        for (Map.Entry<Integer, Set<String>> e : senderRequestIdsByOtherCluster.entrySet()) {
            copy.put(e.getKey(), Set.copyOf(e.getValue()));
        }
        return Collections.unmodifiableMap(copy);
    }

    /**
     * Return a flattened snapshot set of all request IDs that are waiting for ACK
     * across all other clusters (i.e., union of all values in senderRequestIdsByOtherCluster).
     */
    public Set<String> getRequestsWaitingForAck() {
        Set<String> flat = new HashSet<>();
        for (Set<String> s : senderRequestIdsByOtherCluster.values()) {
            flat.addAll(s);
        }
        return Set.copyOf(flat);
    }
}
