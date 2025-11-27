package org.example.statemachine;

/**
 * Sealed hierarchy mirroring the Operation oneof in the proto: either a Transfer or a BalanceRequest.
 * This avoids a manual discriminator enum and enables exhaustive switches.
 */
public sealed interface StateMachineOperation permits TransferOp, BalanceRequestOp {

    /** Simple visitor for exhaustive handling without instanceof checks. */
    interface Visitor<StateMachineOperationResult> {
        StateMachineOperationResult onTransfer(int sender, int receiver, double amount);
        StateMachineOperationResult onBalanceRequest(int accountId);
    }

    /**
     * Dispatch to the appropriate visitor method using sealed-pattern matching.
     */
    default <R> R accept(Visitor<R> visitor) {
        return switch (this) {
            case TransferOp t -> visitor.onTransfer(t.sender(), t.receiver(), t.amount());
            case BalanceRequestOp b -> visitor.onBalanceRequest(b.accountId());
        };
    }

    // Optional convenience factories
    static StateMachineOperation transfer(int sender, int receiver, double amount) {
        return new TransferOp(sender, receiver, amount);
    }

    static StateMachineOperation balanceRequest(int accountId) {
        return new BalanceRequestOp(accountId);
    }
}
