package org.example.tpc;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.TPCAckMessage;
import org.example.TPCCommitMessage;
import org.example.TPCAbortMessage;
import org.example.messaging.ServerMessage;
import org.example.messaging.TPCMessageSender;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Consumer;

public class MessageRetryManager {

    private static final Logger logger = LogManager.getLogger(MessageRetryManager.class);

    private enum MessageType {
        COMMIT,
        ABORT
    }

    private final TPCMessageSender messageSender;
    private final long intervalMillis;
    private final ScheduledExecutorService scheduler;

    // One retry loop per messageId
    private final ConcurrentMap<String, ScheduledFuture<?>> tasks = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Boolean> firstSendCommitDone = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Boolean> firstSendAbortDone = new ConcurrentHashMap<>();

    private final Consumer<String> onAckReceived;

    public MessageRetryManager(TPCMessageSender messageSender,
                               long intervalMillis,
                               ScheduledExecutorService scheduler,
                               Consumer<String> onAckReceived) {
        this.messageSender = Objects.requireNonNull(messageSender, "messageSender");
        this.intervalMillis = intervalMillis;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.onAckReceived = Objects.requireNonNull(onAckReceived, "onAckReceived");
    }

    // ---------- COMMIT flow ----------

    public void startCommitRetries(int targetNodeId,
                                   ServerMessage<TPCCommitMessage> commit) {
        String messageId = commit.getMessageId();

        // Ensure we only create one task per messageId, even if multiple threads call this.
        firstSendCommitDone.putIfAbsent(messageId, Boolean.FALSE);

        Runnable sendTask = () -> {
            // If we were cancelled concurrently, just exit.
            if (!tasks.containsKey(messageId)) {
                return;
            }

            boolean alreadySentFirst =
                    firstSendCommitDone.getOrDefault(messageId, Boolean.FALSE);

            logger.info("Retrying COMMIT for messageId={} (firstSendDone={})",
                    messageId, alreadySentFirst);

            StreamObserver<TPCAckMessage> observer =
                    createAckObserver(MessageType.COMMIT, messageId);

            if (!alreadySentFirst) {
                messageSender.sendCommit(targetNodeId, commit, observer);
                firstSendCommitDone.put(messageId, Boolean.TRUE);
            } else {
                messageSender.broadcastCommitToCluster(targetNodeId, commit, observer);
            }
        };

        ScheduledFuture<?> newFuture =
                scheduler.scheduleAtFixedRate(sendTask, 0, intervalMillis, TimeUnit.MILLISECONDS);

        // Atomically register the task if absent; cancel the new one if some other thread won.
        ScheduledFuture<?> existing = tasks.putIfAbsent(messageId, newFuture);
        if (existing != null) {
            newFuture.cancel(false);
        }
    }

    public void stopCommitRetries(ServerMessage<TPCCommitMessage> commit) {
        stopRetries(MessageType.COMMIT, commit.getMessageId());
    }

    // ---------- ABORT flow ----------

    public void startAbortRetries(int targetNodeId,
                                  ServerMessage<TPCAbortMessage> abort) {
        String messageId = abort.getMessageId();

        firstSendAbortDone.putIfAbsent(messageId, Boolean.FALSE);

        Runnable sendTask = () -> {
            if (!tasks.containsKey(messageId)) {
                return;
            }

            boolean alreadySentFirst =
                    firstSendAbortDone.getOrDefault(messageId, Boolean.FALSE);

            logger.info("Retrying ABORT for messageId={} (firstSendDone={})",
                    messageId, alreadySentFirst);

            StreamObserver<TPCAckMessage> observer =
                    createAckObserver(MessageType.ABORT, messageId);

            if (!alreadySentFirst) {
                messageSender.sendAbort(targetNodeId, abort, observer);
                firstSendAbortDone.put(messageId, Boolean.TRUE);
            } else {
                messageSender.broadcastAbortToCluster(targetNodeId, abort, observer);
            }
        };

        ScheduledFuture<?> newFuture =
                scheduler.scheduleAtFixedRate(sendTask, 0, intervalMillis, TimeUnit.MILLISECONDS);

        ScheduledFuture<?> existing = tasks.putIfAbsent(messageId, newFuture);
        if (existing != null) {
            newFuture.cancel(false);
        }
    }

    public void stopAbortRetries(ServerMessage<TPCAbortMessage> abort) {
        stopRetries(MessageType.ABORT, abort.getMessageId());
    }

    // ---------- Common helpers ----------

    private StreamObserver<TPCAckMessage> createAckObserver(MessageType type,
                                                            String messageId) {
        return new StreamObserver<>() {
            @Override
            public void onNext(TPCAckMessage value) {
                logger.info("Received TPCAck for {} messageId={}", type, messageId);
                stopRetries(type, messageId);
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("{} retry failed for messageId={}: {}",
                        type, messageId, t.getMessage());
                // keep retrying; do not cancel here
            }

            @Override
            public void onCompleted() {
                // no-op
            }
        };
    }

    private void stopRetries(MessageType type, String messageId) {
        // Remove and cancel atomically; safe if called concurrently with start/retry.
        ScheduledFuture<?> fut = tasks.remove(messageId);
        if (fut != null) {
            fut.cancel(false);
        }
        if (type == MessageType.COMMIT) {
            firstSendCommitDone.remove(messageId);
        } else {
            firstSendAbortDone.remove(messageId);
        }
        onAckReceived.accept(messageId);
    }

    public void shutdown() {
        for (Map.Entry<String, ScheduledFuture<?>> e : tasks.entrySet()) {
            ScheduledFuture<?> fut = tasks.remove(e.getKey());
            if (fut != null) {
                fut.cancel(false);
            }
        }
        firstSendCommitDone.clear();
        firstSendAbortDone.clear();
    }
}
