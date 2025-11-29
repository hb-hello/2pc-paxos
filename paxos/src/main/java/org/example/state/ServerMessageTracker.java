package org.example.state;

import com.google.protobuf.MessageLite;
import org.example.messaging.ServerMessage;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks all ServerMessage instances seen by this Paxos node.
 * - De-duplicates by messageId.
 * - Tracks consensus by messageIndex with quorum checks.
 *
 * Thread-safe for concurrent access.
 */
public final class ServerMessageTracker {

    // messageId -> ServerMessage (for de-duplication and optional inspection)
    private final ConcurrentMap<String, ServerMessage<? extends MessageLite>> messages =
            new ConcurrentHashMap<>();

    // messageIndex -> vote counter + quorum flag
    private final ConcurrentMap<String, ConsensusCounter> consensusByIndex =
            new ConcurrentHashMap<>();

    private static final class ConsensusCounter {
        private final AtomicInteger votes = new AtomicInteger(0);
        private final AtomicBoolean quorumSignalled = new AtomicBoolean(false);

        /**
         * Increment votes and return true ONLY the first time
         * votes >= quorumNeeded and this method is called.
         */
        boolean addVoteAndMaybeSignal(int quorumNeeded) {
            int v = votes.incrementAndGet();
            if (v >= quorumNeeded) {
                // Only one thread flips from false -> true.
                return quorumSignalled.compareAndSet(false, true);
            }
            return false;
        }

        /**
         * Read-only check: has quorum been reached (now or earlier)?
         */
        boolean hasQuorum(int quorumNeeded) {
            return quorumSignalled.get() || votes.get() >= quorumNeeded;
        }
    }

    public boolean addMessage(ServerMessage<? extends MessageLite> message) {
        Objects.requireNonNull(message, "message");
        String id = message.getMessageId(); // from updated ServerMessage [attached_file:1]
        ServerMessage<? extends MessageLite> existing = messages.putIfAbsent(id, message);
        return existing == null;
    }

    public boolean hasMessage(ServerMessage<? extends MessageLite> message) {
        Objects.requireNonNull(message, "message");
        return hasMessage(message.getMessageId());
    }

    public boolean hasMessage(String messageId) {
        Objects.requireNonNull(messageId, "messageId");
        return messages.containsKey(messageId);
    }

    /**
     * Add a message and update consensus state for its messageIndex.
     *
     * Semantics:
     * - Returns true exactly ONCE per messageIndex, the first time quorum is reached.
     * - Returns false for:
     *   - duplicates (same messageId),
     *   - additional distinct messages after quorum is already reached.
     */
    public boolean addMessageWithConsensus(ServerMessage<? extends MessageLite> message,
                                           int quorumNeeded) {
        Objects.requireNonNull(message, "message");

        // 1) Never signal on duplicates.
        if (hasMessage(message)) {
            return false;
        }

        // 2) Try to record this messageId. If a race just inserted it, treat as duplicate.
        boolean added = addMessage(message);
        if (!added) {
            return false;
        }

        // 3) New unique messageId; count it toward consensus by messageIndex.
        String index = message.getMessageIndex();
        ConsensusCounter counter = consensusByIndex
                .computeIfAbsent(index, k -> new ConsensusCounter());

        // Returns true only for the first thread that both reaches quorum
        // and flips quorumSignalled from false -> true.
        return counter.addVoteAndMaybeSignal(quorumNeeded);
    }

    /**
     * Read-only check: has this index reached quorum (possibly earlier)?
     */
    public boolean quorumCheck(String messageIndex, int quorumNeeded) {
        Objects.requireNonNull(messageIndex, "messageIndex");
        ConsensusCounter counter = consensusByIndex.get(messageIndex);
        if (counter == null) {
            return false;
        }
        return counter.hasQuorum(quorumNeeded);
    }
}
