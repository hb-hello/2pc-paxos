package org.example.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.state.OperationStatus;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class PaxosMetricsListener implements MetricsListener {
    private static final Logger metricsLogger = LogManager.getLogger("PaxosMetricsLogger");

    private static final ConcurrentMap<Long, StageTimestamps> timings = new ConcurrentHashMap<>();

    private record StageTimestamps(
            AtomicLong noneTime,
            AtomicLong acceptedTime,
            AtomicLong committedTime,
            AtomicLong executedTime,
            AtomicLong checkpointedTime
    ) {}

    @Override
    public void onStatusTransition(long seqNum, OperationStatus from, OperationStatus to, long timestamp) {
        StageTimestamps ts = timings.computeIfAbsent(seqNum, ignored -> new StageTimestamps(
                new AtomicLong(), new AtomicLong(), new AtomicLong(), new AtomicLong(), new AtomicLong()));

        switch (to) {
            case ACCEPTED -> ts.acceptedTime.set(timestamp);
            case COMMITTED -> ts.committedTime.set(timestamp);
            case EXECUTED -> ts.executedTime.set(timestamp);
            case CHECKPOINTED -> ts.checkpointedTime.set(timestamp);
        }
    }

    @Override
    public void printMetrics() {
        metricsLogger.info("Logging Paxos sequence metrics for the most recent 100 operations:");

        // Per-sequence debug (only those that have at least ACCEPTED or beyond)
        //        timings.entrySet().stream()
        //                .filter(e -> e.getValue().acceptedTime.get() > 0L
        //                        || e.getValue().committedTime.get() > 0L
        //                        || e.getValue().executedTime.get() > 0L
        //                        || e.getValue().checkpointedTime.get() > 0L)
        //                .sorted((a, b) -> Long.compare(b.getKey(), a.getKey()))
        //                .limit(100)
        //                .forEach(entry -> logSeqMetrics(entry.getKey(), entry.getValue()));

        // Global averages for each segment; each segment has its own sample count.
        SegmentStats accToCom  = new SegmentStats();
        SegmentStats comToExe  = new SegmentStats();

        for (var entry : timings.entrySet()) {
            var ts = entry.getValue();
            long accepted    = ts.acceptedTime.get();
            long committed   = ts.committedTime.get();
            long executed    = ts.executedTime.get();

            // ACCEPTED -> COMMITTED
            if (accepted > 0L && committed > 0L && committed > accepted) {
                accToCom.add((committed - accepted) / 1_000_000.0);
            }

            // COMMITTED -> EXECUTED
            if (committed > 0L && executed > 0L && executed > committed) {
                comToExe.add((executed - committed) / 1_000_000.0);
            }
        }

        String summary = String.format(
                """
                Average Paxos metrics
                  Samples   acc->com=%d  com->exe=%d
                  Durations (ms)
                      acc->com   avg=%s  min=%s  max=%s
                      com->exe   avg=%s  min=%s  max=%s
                """.stripTrailing(),
                accToCom.count(),
                comToExe.count(),
                formatDuration(accToCom.average()),
                formatDuration(accToCom.min()),
                formatDuration(accToCom.max()),
                formatDuration(comToExe.average()),
                formatDuration(comToExe.min()),
                formatDuration(comToExe.max())
        );

        metricsLogger.info("{}", summary);
    }

    // private helper stubs retained for potential future per-sequence diagnostics.
    // static void printSeqMetrics(long seq, StageTimestamps ts) { ... }
    // static void logSeqMetrics(long seq, StageTimestamps ts) { ... }

    private static String formatDuration(double value) {
        return Double.isNaN(value) ? "N/A" : String.format("%.3f", value);
    }

    private static final class SegmentStats {
        private long count;
        private double total;
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;

        void add(double value) {
            total += value;
            count++;
            min = Math.min(min, value);
            max = Math.max(max, value);
        }

        long count() {
            return count;
        }

        double average() {
            return count > 0 ? total / count : Double.NaN;
        }

        double min() {
            return count > 0 ? min : Double.NaN;
        }

        double max() {
            return count > 0 ? max : Double.NaN;
        }
    }
}
