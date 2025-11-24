package org.example.statemachine;

import org.example.OperationResult;

public final class StateMachineOperationResultMapper {
    private StateMachineOperationResultMapper() {}

    public static OperationResult toProto(StateMachineOperationResult result) {
        var builder = OperationResult.newBuilder();
        if (result.isSuccess()) {
            builder.setSuccess(result.success());
        } else if (result.isBalance()) {
            builder.setBalance(result.balance());
        } else {
            throw new IllegalArgumentException("SimpleOperationResult must have exactly one branch set");
        }
        return builder.build();
    }

    public static StateMachineOperationResult fromProto(OperationResult proto) {
        return switch (proto.getResultCase()) {
            case SUCCESS -> StateMachineOperationResult.success(proto.getSuccess());
            case BALANCE -> StateMachineOperationResult.balance(proto.getBalance());
            case RESULT_NOT_SET -> throw new IllegalArgumentException("OperationResult.oneof 'op' not set");
        };
    }
}

