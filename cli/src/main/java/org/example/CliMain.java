package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.example.benchmark.ClientBenchmark;
import org.example.client.ClientNode;
import org.example.client.TransactionSet;
import org.example.client.TransactionSetLoader;
import org.example.config.Config;
import org.example.messaging.CLIMessageSender;
import org.example.messaging.CLIServiceClient;
import org.example.messaging.MessageReceiver;
import org.example.persistence.DBHandler;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CliMain {

    private final ExecutorManager executorManager;
    private final DBHandler dbHandler;
    private final CLIMessageSender cliMessageSender;
    private final MessageReceiver messageReceiver;
    private final List<TransactionSet> transactionSets;
    private final CountDownLatch warmupComplete = new CountDownLatch(1);
    private final AtomicBoolean warmupStarted = new AtomicBoolean(false);

    public CliMain() {
        // Instantiate DBHandler so CLI commands can query DB contents
        this.executorManager = new ExecutorManager(Config.getNodes().size() - 1);
        this.dbHandler = new DBHandler();

        this.cliMessageSender = new CLIMessageSender(0, Executors.newSingleThreadExecutor());
        CLIServiceClient cliServiceClient = new CLIServiceClient(this);
        this.messageReceiver = new MessageReceiver(0, Config.getClientPort(), List.of(cliServiceClient));

        // Pre-load all transaction sets from CSV
        try {
            TransactionSetLoader loader = new TransactionSetLoader();
            this.transactionSets = loader.loadAll(); // uses Config.getTransactionsSetsPath()[attached_file:1]
        } catch (IOException e) {
            throw new RuntimeException("Failed to load transaction sets", e);
        }

        startAllServers();
    }

    public void start() {
        try {
            executorManager.submitListeningTask(() -> messageReceiver.startListening(() -> {
            }));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void resetAllServers() {
        for (int i = 1; i <= Config.getServerCount(); i++) {
            try {
                cliMessageSender.sendReset(i);
            } catch (Exception e) {
                System.out.println("Warning: failed to reset server " + i + " " + e.getMessage());
            }
        }
    }

    public void activateAllServers() {
        try {
            System.out.println("Waiting briefly before activating all servers for client warmup...");
            Thread.sleep(500); // brief pause to ensure any prior operations have settled
            for (int i = 1; i <= Config.getServerCount(); i++) {
                try {
                    cliMessageSender.sendActiveFlag(i, true);
                } catch (Exception e) {
                    System.out.println("Warning: failed to activate server " + i + " " + e.getMessage());
                }
            }
            System.out.println("Server activation complete.");
        } catch (Exception e) {
            System.out.println("Warning: failed to activate servers before warmup: " + e.getMessage());
        }
    }

    /**
     * Deactivates all server nodes and activates only the given server IDs.
     */
    private void activateServers(List<Integer> activeServerIds) {
        try {
            // Deactivate all servers first
            for (int i = 1; i <= Config.getServerCount(); i++) {
                cliMessageSender.sendActiveFlag(i, false);
            }

            // Activate only the specified servers
            for (int serverId : activeServerIds) {
                try {
                    cliMessageSender.sendActiveFlag(serverId, true);
                } catch (Exception e) {
                    System.out.println("Warning: failed to activate server " + serverId + " " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.out.println("Error while (de)activating servers: " + e.getMessage());
        }
    }

    private void startAllServers() {
        try {
            ServerManager.startAllServers(Config.getServerExecutablePath(), Config.getServerCount());

            try {
                start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void warmup() {
        if (!warmupStarted.compareAndSet(false, true)) {
            return;
        }
        executorManager.submitMessageProcessing(() -> {
            try {
                cliMessageSender.warmup();
            } finally {
                warmupComplete.countDown();
            }
        });
    }

    /**
     * Process a single transaction set by set number:
     *  - find the TransactionSet
     *  - activate the correct live servers
     *  - build a ClientNode using DBHandler's account→cluster mapping
     *  - send all transactions in order
     */
    private void processTransactionSet(int setNumber) {
        if (transactionSets == null || transactionSets.isEmpty()) {
            System.out.println("No transaction sets loaded.");
            return;
        }

        TransactionSet set = transactionSets.stream()
                .filter(ts -> ts.setNumber() == setNumber)
                .findFirst()
                .orElse(null);

        if (set == null) {
            System.out.printf("Transaction set %d not found.%n", setNumber);
            return;
        }

        System.out.printf("Processing set %d with %d transactions, live nodes: %s%n",
                setNumber, set.transactions().size(), set.liveNodes());

        // Live nodes in CSV are like "n1, n2, n3" or "[n1, n2, ...]"[attached_file:2]
        List<Integer> activeServerIds = set.liveNodes();
        activateServers(activeServerIds);

        // Fetch account→cluster mapping from DBHandler
        Map<Integer, Integer> accountToClusterIndex = dbHandler.getAccountIdToClusterIndex();

        // Create a fresh ClientNode for this set
        ClientNode clientNode = new ClientNode(0, cliMessageSender, accountToClusterIndex);

        resetAllServers();

        activateServers(set.liveNodes());

        // Send all transactions in order
        for (String tx : set.transactions()) {
            clientNode.processTransaction(tx);
        }

        System.out.printf("Finished processing set %d%n", setNumber);
    }

    private void printDBForAccountId(String idStr) {
        try {
            int id = Integer.parseInt(idStr);
            String result = dbHandler.getAccountEntry(id);
            System.out.println(result);
        } catch (NumberFormatException nfe) {
            System.out.println("Invalid client ID: must be an integer.");
        } catch (Exception e) {
            System.out.println("Error fetching account entry: " + e.getMessage());
        }
    }

    private void fetchAndPrintDB() {
        try {
            cliMessageSender.printDB();
        } catch (Exception e) {
            System.out.println("Error fetching DB contents: " + e.getMessage());
        }
    }

    // In CliMain.java

    private void runBenchmark(int totalRequests) {
        // Make sure servers are reset and active
//        resetAllServers();
        activateAllServers();

        // Build a fresh ClientNode with current account→cluster mapping
        Map<Integer, Integer> accountToClusterIndex = dbHandler.getAccountIdToClusterIndex();
        ClientNode clientNode = new ClientNode(0, cliMessageSender, accountToClusterIndex);

        ClientBenchmark benchmark = new ClientBenchmark(totalRequests);
        clientNode.setMetricsListener(benchmark);

        // Build 6000 intra-shard transfers evenly across shards
        List<String> txs = ClientBenchmark.buildIntraShardTransfers(accountToClusterIndex, totalRequests);

        System.out.printf("Starting benchmark: %d intra-shard transfers%n", totalRequests);

        benchmark.start();
        for (String tx : txs) {
            clientNode.processTransaction(tx);
        }

        try {
            // Wait up to e.g. 60 seconds for all replies
            benchmark.awaitCompletion(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Benchmark interrupted.");
            return;
        }

        benchmark.printResults();
    }


    private void shutdown() {
        try {
            dbHandler.shutdown();
            messageReceiver.shutdown();
        } catch (Exception ignored) {
        }
    }

    private void awaitWarmup() {
        try {
            warmupComplete.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for warmup", e);
        }
    }

    public static void main(String[] args) {
        // Initialize config first so we know settings if needed
        Config.initialize();

        // Set up a CLI-specific log file path BEFORE any class that uses LogManager is loaded.
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss.SSS"));
        String cliLogPath = "logs/cli/log-" + timestamp + ".log";
        System.setProperty("cliLogPath", cliLogPath);

        // Ensure parent directory exists
        File parent = new File(cliLogPath).getParentFile();
        if (parent != null && !parent.exists()) {
            try {
                java.nio.file.Files.createDirectories(parent.toPath());
            } catch (IOException ioe) {
                System.err.println("Failed to create CLI log directory '" + parent.getAbsolutePath() + "': " + ioe.getMessage());
            }
        }

        // Reconfigure Log4j2 to pick up the CLI log path before any static loggers are initialized
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.reconfigure();

        CliMain cli = new CliMain();
        System.out.println("Waiting for CLI warmup to complete...");
        cli.awaitWarmup();
        System.out.println("Warmup complete. CLI is ready.");

        int nextSetNumber = 1;
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.println();
            System.out.println("Options:");
            System.out.println(" 1 - PrintDB");
            System.out.println(" 2 - PrintLog");
            System.out.println(" 3 - PrintStatus");
            System.out.println(" 4 - PrintView");
            System.out.println(" 5 - Continue with next set (#" + nextSetNumber + ")");
            System.out.println(" 6 - DEBUG: PrintOperationLog");
            System.out.println(" 7 - DEBUG: Pause/Resume client (pause a client to inspect logs/db)");
            System.out.println(" 8 - DEBUG: Choose next set number");
            System.out.println(" 9 - DEBUG: PrintDB directly");
            System.out.println(" 10 - Run synthetic benchmark (6000 intra-shard tx)");
            System.out.println(" 0 - Exit");
            System.out.print("Choice: ");
            String choice = sc.nextLine().trim();

            switch (choice) {
                case "1" -> {
                    System.out.println("Printing DB contents from all servers:");
                    cli.fetchAndPrintDB();
                }
                case "5" -> {
                    System.out.println("Processing transaction set #" + (nextSetNumber));
                    cli.processTransactionSet(nextSetNumber);
                    nextSetNumber++;
                }
                case "9" -> {
                    System.out.print("Enter client ID: ");
                    String idStr = sc.nextLine().trim();
                    cli.printDBForAccountId(idStr);
                }
                case "10" -> {
                    cli.runBenchmark(6000);
                }
                case "0" -> {
                    System.out.println("Exiting...");
                    cli.shutdown();
                    return;
                }

                default -> System.out.println("Unknown choice.");
            }
        }
    }
}