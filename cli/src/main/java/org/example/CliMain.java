package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.example.benchmark.ClientBenchmark;
import org.example.benchmark.ClientMetricsListener;
import org.example.benchmark.ContentionBenchmarkSuite;
import org.example.client.ClientNode;
import org.example.client.TransactionSet;
import org.example.client.TransactionSetLoader;
import org.example.config.Config;
import org.example.messaging.CLIMessageSender;
import org.example.messaging.CLIServiceClient;
import org.example.messaging.ClientServiceClient;
import org.example.messaging.MessageReceiver;
import org.example.persistence.DBHandler;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Thread.sleep;

public class CliMain {

    private static final Logger logger = LogManager.getLogger(CliMain.class);

    private final ExecutorManager executorManager;
    private final DBHandler dbHandler;
    private final CLIMessageSender cliMessageSender;
    private final MessageReceiver messageReceiver;
    private final List<TransactionSet> transactionSets;
    private final CountDownLatch warmupComplete = new CountDownLatch(1);
    private final AtomicBoolean warmupStarted = new AtomicBoolean(false);
    private final ClientServiceClient clientServiceClient;

    private volatile ClientNode activeClientNode;
    private volatile ClientBenchmark activeBenchmark;

    public CliMain() {
        // Instantiate DBHandler so CLI commands can query DB contents
        this.executorManager = new ExecutorManager(Config.getNodes().size() - 1);
        this.dbHandler = new DBHandler();

        this.cliMessageSender = new CLIMessageSender(0, Executors.newSingleThreadExecutor());
        CLIServiceClient cliServiceClient = new CLIServiceClient(this);
        this.clientServiceClient = new ClientServiceClient(this);
        this.messageReceiver = new MessageReceiver(0, Config.getClientPort(), List.of(cliServiceClient, clientServiceClient));

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
            sleep(500); // brief pause to ensure any prior operations have settled
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

    public void warmupWithPings() {
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
     * - find the TransactionSet
     * - activate the correct live servers
     * - build a ClientNode using DBHandler's account→cluster mapping
     * - send all transactions in order
     * - track throughput and latency using ClientBenchmark
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

        // Fetch account→cluster mapping from DBHandler
        Map<Integer, Integer> accountToClusterIndex = dbHandler.getAccountIdToClusterIndex();

        // Create a fresh ClientNode for this set
        ClientNode clientNode = new ClientNode(cliMessageSender, accountToClusterIndex, executorManager.getRetryExecutor());
        registerActiveClientNode(clientNode);

        // Create and register a benchmark to track metrics
        int totalRequests = set.transactions().size();
        ClientBenchmark benchmark = new ClientBenchmark(totalRequests);
        clientNode.setMetricsListener(benchmark);
        this.activeBenchmark = benchmark;

        dbHandler.resetDatabases();
        resetAllServers();
        activateServers(set.liveNodes());

        // Start timing and send all transactions
        benchmark.start();
        for (String tx : set.transactions()) {
            clientNode.processTransaction(tx);
        }

        try {
            benchmark.awaitCompletion(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.out.println("Interrupted while waiting for transaction set completion.");
            return;
        }

        System.out.printf("Finished sending set %d. Use option 3 to view performance stats.%n", setNumber);
    }

    /**
     * Print performance statistics from the active benchmark.
     */
    private void printPerformanceStats() {
        ClientBenchmark benchmark = this.activeBenchmark;
        if (benchmark == null) {
            System.out.println("No active benchmark. Run a transaction set first (option 5).");
            return;
        }
        benchmark.printResults();
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

    private void fetchAndPrintLog(int serverId) {
        try {
            cliMessageSender.printOperationLog(serverId);
        } catch (Exception e) {
            System.out.println("Error fetching operation log for server " + serverId + ": " + e.getMessage());
        }
    }

    private void fetchAndPrintTrackedRequests(int serverId) {
        try {
            cliMessageSender.printTrackedRequests(serverId);
        } catch (Exception e) {
            System.out.println("Error fetching tracked requests for server " + serverId + ": " + e.getMessage());
        }
    }

    private void fetchAndPrintNewView() {
        try {
            cliMessageSender.printNewView();
        } catch (Exception e) {
            System.out.println("Error fetching new view information: " + e.getMessage());
        }
    }

    private void runBenchmark(int totalRequests, double skew) {
        // Make sure servers are reset and active
        resetAllServers();
        activateAllServers();

        // Build a fresh ClientNode with current account→cluster mapping
        Map<Integer, Integer> accountToClusterIndex = dbHandler.getAccountIdToClusterIndex();
        ClientNode clientNode = new ClientNode(cliMessageSender, accountToClusterIndex, executorManager.getRetryExecutor());
        registerActiveClientNode(clientNode);

        ClientBenchmark benchmark = new ClientBenchmark(totalRequests);
        clientNode.setMetricsListener(benchmark);

        // Build intra-shard transfers evenly across shards
        List<String> txs = ClientBenchmark.buildIntraShardTransfersWithSkew(accountToClusterIndex, totalRequests, skew);

        System.out.printf("Starting benchmark: %d intra-shard transfers%n", totalRequests);

        benchmark.start();
        for (String tx : txs) {
            clientNode.processTransaction(tx);
        }

        try {
            int secondsToWait = Math.max(totalRequests / 500, 10);
            benchmark.awaitCompletion(secondsToWait, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Benchmark interrupted.");
            return;
        }

        benchmark.printResults();
    }

    private void runBenchmarkWithWarmup(int totalRequests, double skew) {
        resetAllServers();
        activateAllServers();

        // Wait for leaders to stabilize
        System.out.println("Waiting for leader election to stabilize...");
        try {
            sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Map<Integer, Integer> accountToClusterIndex = dbHandler.getAccountIdToClusterIndex();
        ClientNode clientNode = new ClientNode(cliMessageSender, accountToClusterIndex, executorManager.getRetryExecutor());
        registerActiveClientNode(clientNode);

        warmupWithTransactions(accountToClusterIndex, clientNode, 90);

        ClientBenchmark benchmark = new ClientBenchmark(totalRequests);
        clientNode.setMetricsListener(benchmark);

        List<String> txs = ClientBenchmark.buildIntraShardTransfersWithSkew(accountToClusterIndex, totalRequests, skew);
    }


    private void runContentionSuite(int transactionsPerDataset) {
        Map<Integer, Integer> accountToClusterIndex = dbHandler.getAccountIdToClusterIndex();

        ContentionBenchmarkSuite suite = new ContentionBenchmarkSuite(
                (transactions, skew) -> executeSingleBenchmark(transactions, accountToClusterIndex),
                accountToClusterIndex
        );

        suite.runSuite(transactionsPerDataset);
        suite.printSummary();
        suite.exportResults();
        suite.generateCharts();  // Generate both charts
    }


    private ContentionBenchmarkSuite.BenchmarkResult executeSingleBenchmark(List<String> transactions,
                                                                            Map<Integer, Integer> accountToClusterIndex) {
        resetAllServers();
        activateAllServers();

        // Wait for leaders to stabilize
        System.out.println("Waiting for leader election to stabilize...");
        try {
            sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        ClientNode clientNode = new ClientNode(cliMessageSender, accountToClusterIndex, executorManager.getRetryExecutor());
        registerActiveClientNode(clientNode);

        warmupWithTransactions(accountToClusterIndex, clientNode, 90);

        int totalRequests = transactions.size();
        ClientBenchmark benchmark = new ClientBenchmark(totalRequests);
        clientNode.setMetricsListener(benchmark);

        benchmark.start();
        for (String tx : transactions) {
            clientNode.processTransaction(tx);
        }

        try {
            int secondsToWait = Math.max(totalRequests / 200, 10);
            benchmark.awaitCompletion(secondsToWait, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Extract metrics from benchmark
        return extractResult(benchmark, totalRequests);
    }

    private void warmupWithTransactions(Map<Integer, Integer> accountToClusterIndex, ClientNode clientNode, int warmupTxCount) {
        // Warm-up phase - send a few transactions without measuring
        System.out.println("Running warm-up transactions...");
        List<String> warmupTxs = ClientBenchmark.buildIntraShardTransfersWithSkew(accountToClusterIndex, warmupTxCount, 0.1);
        CountDownLatch warmupLatch = new CountDownLatch(warmupTxs.size());
        clientNode.setMetricsListener(new ClientMetricsListener() {
            @Override
            public void onRequestCompleted(ClientRequest request, long latencyMillis) {
                warmupLatch.countDown();
            }

            @Override
            public void onRequestAborted(ClientRequest request, long latencyMillis) {
                warmupLatch.countDown();
            }
        });
        for (String tx : warmupTxs) {
            clientNode.processTransaction(tx);
        }
        try {
            warmupLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Public overload for warmup with transactions that creates its own ClientNode.
     * Can be called right after cli.awaitWarmup() in main method.
     */
    public void warmupWithTransactions(int warmupTxCount) {
        Map<Integer, Integer> accountToClusterIndex = dbHandler.getAccountIdToClusterIndex();
        ClientNode clientNode = new ClientNode(cliMessageSender, accountToClusterIndex, executorManager.getRetryExecutor(), false);
        registerActiveClientNode(clientNode);
        resetAllServers();
        activateAllServers();
        warmupWithTransactions(accountToClusterIndex, clientNode, warmupTxCount);
        System.out.println("Transaction warmup complete.");
    }

    private ContentionBenchmarkSuite.BenchmarkResult extractResult(ClientBenchmark benchmark, int totalRequests) {
        return new ContentionBenchmarkSuite.BenchmarkResult(
                0.0,  // skew will be set by suite
                0,    // run number will be set by suite
                totalRequests,
                (int) benchmark.getCompletedCount(),
                benchmark.getElapsedSeconds(),
                benchmark.getThroughput(),
                benchmark.getAvgLatencyMs(),
                benchmark.getMinLatencyMs(),
                benchmark.getMaxLatencyMs()
        );
    }

    private void printReshard(int setNumber) {
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

        int clusterCount = Config.getServerClusterCount();  // likely 3
        Map<Integer, Integer> current = dbHandler.getAccountIdToClusterIndex();
        List<String> recentTxs = set.transactions();
        List<ReshardingPlanner.Move> moves = ReshardingPlanner.computeReshardingMoves(current, recentTxs, clusterCount);
        if (moves.isEmpty()) {
            System.out.println("No resharding moves suggested based on recent transactions.");
        } else {
            System.out.println("Applying suggested resharding moves:");
            for (ReshardingPlanner.Move move : moves) {
                System.out.println(move);
            }

            // Execute moves using DBHandler
            System.out.println("Executing resharding moves now...");
            for (ReshardingPlanner.Move move : moves) {
                int accountId = move.accountId();
                int targetCluster = move.newCluster();
                try {
                    dbHandler.moveAccount(accountId, targetCluster);
                    // Update local view
                    current.put(accountId, targetCluster);
//                    System.out.printf("Moved account %d -> cluster %d\n", accountId, targetCluster);
                } catch (Exception e) {
                    System.out.printf("Failed to move account %d -> cluster %d: %s\n", accountId, targetCluster, e.getMessage());
                }
            }
            System.out.println("Resharding moves execution completed.");
        }
    }

    private void shutdown() {
        try {
            dbHandler.shutdown();
            messageReceiver.shutdown();
        } catch (Exception ignored) {
        }
    }

    void registerActiveClientNode(ClientNode clientNode) {
        this.activeClientNode = clientNode;
    }

    public void handleClientReply(ClientReply reply) {
        ClientNode clientNode = this.activeClientNode;
        if (clientNode == null) {
            logger.warn("Dropping client reply for request id {} because no active client node is registered.",
                    reply.getRequestId());
            return;
        }
        clientNode.handleClientReply(reply);
    }

    private void awaitWarmupWithPings() {
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
        cli.awaitWarmupWithPings();
        cli.warmupWithTransactions(180);
        System.out.println("Warmup complete. CLI is ready.");

        int currentSet = 1;
        int nextSetNumber = 1;
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.println();
            System.out.println("Options:");
            System.out.println(" 1 - PrintDB");
            System.out.println(" 2 - PrintBalance");
            System.out.println(" 3 - Performance");
            System.out.println(" 4 - PrintView");
            System.out.println(" 5 - Continue with next set (#" + nextSetNumber + ")");
            System.out.println(" 6 - PrintReshard");
            System.out.println(" 7 - DEBUG: PrintRequestsTracked");
            System.out.println(" 8 - DEBUG: Choose next set number");
            System.out.println(" 9 - DEBUG: PrintLog");
            System.out.println(" 10 - Run one benchmark");
            System.out.println(" 11 - Run benchmark suite");
            System.out.println(" 0 - Exit");
            System.out.print("Choice: ");
            String choice = sc.nextLine().trim();

            switch (choice) {
                case "1" -> {
                    System.out.println("Printing DB contents from all servers:");
                    cli.fetchAndPrintDB();
                }
                case "2" -> {
                    System.out.print("Enter client ID: ");
                    String idStr = sc.nextLine().trim();
                    cli.printDBForAccountId(idStr);
                }
                case "3" -> {
                    cli.printPerformanceStats();
                }
                case "4" -> {
                    System.out.println("Fetching and printing new view information from all servers:");
                    cli.fetchAndPrintNewView();
                }
                case "5" -> {
                    System.out.println("Processing transaction set #" + (nextSetNumber));
                    cli.processTransactionSet(nextSetNumber);
                    currentSet = nextSetNumber;
                    nextSetNumber++;
                }
                case "6" -> cli.printReshard(currentSet);
                case "7" -> {
                    System.out.print("Enter server ID to print tracked requests from: ");
                    try {
                        int serverId = Integer.parseInt(sc.nextLine().trim());
                        if (serverId <= 0 || serverId > Config.getServerCount()) {
                            System.out.println("Server ID must be positive and less than " + (Config.getServerCount() + 1) + ".");
                            break;
                        }
                        cli.fetchAndPrintTrackedRequests(serverId);
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid server ID.");
                    }
                }
                case "8" -> {
                    System.out.print("Enter set #: ");
                    try {
                        int set = Integer.parseInt(sc.nextLine().trim());
                        if (set <= 0 || set > cli.transactionSets.size()) {
                            System.out.println("Set number must be positive and less than " + cli.transactionSets.size() + ".");
                            break;
                        }
                        cli.processTransactionSet(set);
                        currentSet = set;
                        nextSetNumber = set + 1;
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid set number.");
                    }
                }
                case "9" -> {
                    System.out.print("Enter server ID to print log from: ");
                    try {
                        int serverId = Integer.parseInt(sc.nextLine().trim());
                        if (serverId <= 0 || serverId > Config.getServerCount()) {
                            System.out.println("Server ID must be positive and less than " + (Config.getServerCount() + 1) + ".");
                            break;
                        }
                        cli.fetchAndPrintLog(serverId);
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid server ID.");
                    }
                }
                case "10" -> {
                    System.out.println("Enter number of requests for benchmark (e.g., 300): ");
                    String reqStr = sc.nextLine().trim();
                    int reqCount = Integer.parseInt(reqStr);
                    try {
                        if (reqCount <= 0) {
                            System.out.println("# requests must be positive.");
                            break;
                        }
                        int clusterCount = Config.getServerClusterCount();
                        if (reqCount % clusterCount != 0) {
                            System.out.println("# requests must be divisible by number of clusters");
                            break;
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid request count.");
                        break;
                    }

                    System.out.println("Enter skew percentage for benchmark (e.g., 40): ");
                    int skewPercent = Integer.parseInt(sc.nextLine().trim());
                    if (skewPercent < 0 || skewPercent > 100) {
                        System.out.println("Skew percentage must be between 0 and 100.");
                        break;
                    }
                    double skew = skewPercent / 100.0;
                    cli.runBenchmark(reqCount, skew);
                }
                case "11" -> {
                    System.out.println("Enter number of requests for benchmark suite (e.g., 300): ");
                    String reqStr = sc.nextLine().trim();
                    int reqCount = Integer.parseInt(reqStr);
                    try {
                        if (reqCount <= 0) {
                            System.out.println("# requests must be positive.");
                            break;
                        }
                        int clusterCount = Config.getServerClusterCount();
                        if (reqCount % clusterCount != 0) {
                            System.out.println("# requests must be divisible by number of clusters");
                            break;
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("Invalid request count.");
                        break;
                    }
                    cli.runContentionSuite(reqCount);
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

