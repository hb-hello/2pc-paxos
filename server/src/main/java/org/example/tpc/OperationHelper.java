package org.example.tpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.Operation;
import org.example.Transfer;
import org.example.config.Config;
import org.example.persistence.KeyValueStore;

public final class OperationHelper {
    private static final Logger logger = LogManager.getLogger(OperationHelper.class);

    private OperationHelper() {
    }

    public static ExecutionMode resolveExecutionMode(int serverId,
                                                     Operation operation,
                                                     KeyValueStore<Double> database) {
        try {
            int localCluster = Config.getServerClusterIndex(serverId);
            if (operation.hasTransfer()) {
                Transfer transfer = operation.getTransfer();
                Integer senderCluster = database.getClusterId(transfer.getSender());
                Integer receiverCluster = database.getClusterId(transfer.getReceiver());

                boolean senderLocal = senderCluster != null && senderCluster == localCluster;
                boolean receiverLocal = receiverCluster != null && receiverCluster == localCluster;

                if (senderLocal && receiverLocal) {
                    return ExecutionMode.BOTH;
                } else if (senderLocal) {
                    return ExecutionMode.SENDER;
                } else if (receiverLocal) {
                    return ExecutionMode.RECEIVER;
                } else {
                    logger.warn("Transfer accounts {}->{}, no local cluster data; executing BOTH",
                            transfer.getSender(), transfer.getReceiver());
                    return ExecutionMode.BOTH;
                }
            }
            return ExecutionMode.BOTH;
        } catch (Exception e) {
            logger.warn("Failed to resolve execution mode, defaulting to BOTH: {}", e.getMessage());
            return ExecutionMode.BOTH;
        }
    }

    public static int[] resolveAccountIds(Operation operation, ExecutionMode mode) {
        return switch (operation.getOpCase()) {
            case TRANSFER -> {
                Transfer t = operation.getTransfer();
                int sender = t.getSender();
                int receiver = t.getReceiver();
                yield switch (mode) {
                    case BOTH -> new int[]{sender, receiver};
                    case SENDER -> new int[]{sender};
                    case RECEIVER -> new int[]{receiver};
                };
            }
            case BALANCE_REQUEST -> new int[]{operation.getBalanceRequest().getAccountId()};
            case OP_NOT_SET -> throw new IllegalArgumentException("Operation.op not set");
        };
    }
}

