package org.example.statemachine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.*;
import org.example.persistence.KeyValueStore;
import org.example.tpc.ExecutionMode;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class BankStateMachine implements StateMachine {

    private static final Logger logger = LogManager.getLogger(BankStateMachine.class);

    private final Map<Integer, Double> modifiedBalances;
    private final KeyValueStore<Double> database;

    public BankStateMachine(KeyValueStore<Double> database) {
        this.modifiedBalances = new ConcurrentHashMap<>();
        this.database = database;
//        logger.info("BankStateMachine initialized with balances: {}", this.balances);
    }

    private double requireBalance(int id) {
//        logger.info("requireBalance: Looking up account ID: '{}' (length: {}, type: {})",
//            id, id.length(), id.getClass().getSimpleName());
//        logger.info("requireBalance: Current balances map keys: {}", balances.keySet());
//        logger.info("requireBalance: Current balances map size: {}", balances.size());

        // Check for exact matches and similar keys
//        for (String key : balances.keySet()) {
//            logger.info("requireBalance: Comparing with key: '{}' (length: {}, equals: {})",
//                key, key.length(), key.equals(id));
//        }

        Double v = database.get(id);
//        logger.info("requireBalance: Fetched balance for '{}': {}", id, v);

        if (v == null) {
            logger.error("requireBalance: Account '{}' not present in balances, throwing exception", id);
            throw new IllegalArgumentException("Account not present in balances: " + id);
        }
        return v;
    }

    public void restoreBalance(int accountId, double balance) {
        database.put(accountId, balance);
        modifiedBalances.put(accountId, balance);
        logger.info("restoreBalance: marking account {} as modified (size now {})",
                accountId, modifiedBalances.size());
    }

    @Override
    public OperationResult execute(Operation operation, ExecutionMode mode) {
        StateMachineOperation op = StateMachineOperationMapper.fromProto(operation);

        return op.accept(new StateMachineOperation.Visitor<>() {
            @Override
            public OperationResult onTransfer(int sender, int receiver, double amount) {
                logger.info("Executing transfer: {} -> {} amount {} in mode {}",
                        sender, receiver, amount, mode);
                // No-op transfer, always succeeds and does not mutate state
                if (amount == 0.0) {
                    return StateMachineOperationResultMapper.toProto(
                            StateMachineOperationResult.success(true)
                    );
                }

                if (mode == null) {
                    throw new IllegalArgumentException("ExecutionMode must not be null");
                }

                return switch (mode) {
                    case BOTH -> handleTransferBoth(sender, receiver, amount);
                    case SENDER -> handleTransferSender(sender, amount);
                    case RECEIVER -> handleTransferReceiver(receiver, amount);
                    default -> throw new IllegalStateException("Unexpected execution mode: " + mode);
                };
            }

            @Override
            public OperationResult onBalanceRequest(int accountId) {
                logger.info("Executing balance request for account {} in mode {}", accountId, mode);
                double balance = requireBalance(accountId);
                return StateMachineOperationResultMapper.toProto(
                        StateMachineOperationResult.balance(balance)
                );
            }

            private OperationResult handleTransferBoth(int sender, int receiver, double amount) {
                double fromBal = requireBalance(sender);
                double toBal = requireBalance(receiver);

                if (fromBal < amount) {
                    // Insufficient funds – fail without mutating state
                    return StateMachineOperationResultMapper.toProto(
                            StateMachineOperationResult.success(false)
                    );
                }

                double newFrom = fromBal - amount;
                double newTo = toBal + amount;

                database.put(sender, newFrom);
                database.put(receiver, newTo);

                modifiedBalances.put(sender, newFrom);
                modifiedBalances.put(receiver, newTo);

                return StateMachineOperationResultMapper.toProto(
                        StateMachineOperationResult.success(true)
                );
            }

            private OperationResult handleTransferSender(int sender, double amount) {
                double fromBal = requireBalance(sender);

                if (fromBal < amount) {
                    return StateMachineOperationResultMapper.toProto(
                            StateMachineOperationResult.success(false)
                    );
                }

                double newFrom = fromBal - amount;

                database.put(sender, newFrom);
                modifiedBalances.put(sender, newFrom);

                return StateMachineOperationResultMapper.toProto(
                        StateMachineOperationResult.success(true)
                );
            }

            private OperationResult handleTransferReceiver(int receiver, double amount) {
                double toBal = requireBalance(receiver);

                double newTo = toBal + amount;

                database.put(receiver, newTo);
                modifiedBalances.put(receiver, newTo);

                return StateMachineOperationResultMapper.toProto(
                        StateMachineOperationResult.success(true)
                );
            }
        });
    }


    @Override
    public String snapshot() {
        StringBuilder sb = new StringBuilder();
        modifiedBalances.keySet().stream()
                .sorted()
                .forEach(key -> sb.append(key).append("=").append(modifiedBalances.get(key)).append(";"));
        return sb.toString();
    }

    @Override
    public boolean applySnapshot(String snapshot) {
        String[] entries = snapshot.split(";");
        for (String entry : entries) {
            if (entry.isEmpty()) continue;
            String[] parts = entry.split("=");
            if (parts.length != 2) {
                logger.error("APPLY SNAPSHOT: Invalid entry format: {}", entry);
                return false;
            }
            try {
                int accountId = Integer.parseInt(parts[0]);
                double balance = Double.parseDouble(parts[1]);
                database.put(accountId, balance);
                modifiedBalances.put(accountId, balance);
            } catch (NumberFormatException e) {
                logger.error("APPLY SNAPSHOT: Invalid number format in entry: {}", entry, e);
                return false;
            }
        }
        return true;
    }

    public Set<Integer> getModifiedAccounts() {
        return modifiedBalances.keySet();
    }

    @Override
    public void reset() {
//        logger.info("RESET: Clearing balances and restoring initial state");
        modifiedBalances.clear();
    }
}
