package org.example.benchmark;

import org.example.ClientRequest;

public interface ClientMetricsListener {
    void onRequestCompleted(ClientRequest request, long latencyMillis);
    void onRequestAborted(ClientRequest request, long latencyMillis);
}