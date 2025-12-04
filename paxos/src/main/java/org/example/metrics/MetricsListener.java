package org.example.metrics;

import org.example.state.OperationLog;
import org.example.state.OperationStatus;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public interface MetricsListener {
    void onStatusTransition(long seqNum, OperationStatus from, OperationStatus to, long timestamp);
    void printMetrics();
}