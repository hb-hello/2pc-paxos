package org.example.client;

import java.util.List;

public record TransactionSet(
        int setNumber,
        List<Integer> liveNodes,
        List<String> transactions
) {
    public TransactionSet {
        // Make collections unmodifiable snapshots[web:39][web:43]
        liveNodes = List.copyOf(liveNodes);
        transactions = List.copyOf(transactions);
    }

    @Override
    public String toString() {
        return "TransactionSet{" +
                "setNumber=" + setNumber +
                ", liveNodes=" + liveNodes +
                ", transactionGroups=" + transactions.size() +
                '}';
    }
}

