package org.example.statemachine;

import org.example.BalanceRequest;
import org.example.Operation;
import org.example.Transfer;

/**
 * Utility for converting between sealed StateMachineOperation values and protobuf Operation messages.
 */
public final class StateMachineOperationMapper {
    private StateMachineOperationMapper() {}

    public static Operation toProto(StateMachineOperation op) {
        return switch (op) {
            case TransferOp t -> Operation.newBuilder()
                    .setTransfer(Transfer.newBuilder()
                            .setSender(t.sender())
                            .setReceiver(t.receiver())
                            .setAmount(t.amount())
                            .build())
                    .build();
            case BalanceRequestOp b -> Operation.newBuilder()
                    .setBalanceRequest(BalanceRequest.newBuilder()
                            .setAccountId(b.accountId())
                            .build())
                    .build();
        };
    }

    public static StateMachineOperation fromProto(Operation op) {
        org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(StateMachineOperationMapper.class);
//        logger.info("fromProto: Converting operation, opCase: {}", op.getOpCase());

        return switch (op.getOpCase()) {
            case TRANSFER -> {
                var t = op.getTransfer();
//                logger.info("fromProto: TRANSFER - sender='{}', receiver='{}', amount={}",
//                    t.getSender(), t.getReceiver(), t.getAmount());
                yield new TransferOp(t.getSender(), t.getReceiver(), t.getAmount());
            }
            case BALANCE_REQUEST -> {
                String accountId = op.getBalanceRequest().getAccountId();
//                logger.info("fromProto: BALANCE_REQUEST - accountId='{}'", accountId);
                yield new BalanceRequestOp(accountId);
            }
            case OP_NOT_SET -> {
//                logger.error("fromProto: Operation.oneof 'op' not set");
                throw new IllegalArgumentException("Operation.oneof 'op' not set");
            }
        };
    }
}
