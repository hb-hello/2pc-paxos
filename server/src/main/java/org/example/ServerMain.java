package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.example.config.Config;
import org.example.messaging.CLIServiceServer;
import org.example.messaging.MessageReceiver;
import org.example.messaging.ServerActivityInterceptor;
import org.example.persistence.DatabaseManager;
import org.example.persistence.KeyValueStore;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;

public class ServerMain {
    private final int serverId;
    private final Logger logger;

    private final ExecutorManager executorManager;

    private final KeyValueStore<Double> database;

    private final MessageReceiver messageReceiver;
    private final CLIServiceServer cliServiceServer;

    private TPCServer server;

    public ServerMain(int serverId) {
        this.serverId = serverId;
        // Initialize the instance logger here so Log4j has already been configured in main()
        this.logger = LogManager.getLogger(ServerMain.class);
        this.executorManager = new ExecutorManager(Config.getNodes().size() - 1);
        logger.info("Connecting to database for Server {}", serverId);
        this.database = DatabaseManager.create(serverId);
        logger.info("Database connection established for Server {}", serverId);
        this.cliServiceServer = new CLIServiceServer(this);
        this.server = new TPCServer(serverId, cliServiceServer, executorManager, database);
        this.messageReceiver = new MessageReceiver(serverId, Config.getNodePort(serverId), server.getServices(), new ServerActivityInterceptor());
    }

    @SuppressWarnings("unused")
    public void reset() {
        // potentially set a new serverId here if supporting re-configuration
        this.server = new TPCServer(serverId, cliServiceServer, executorManager, database);
        logger.info("Server {} state has been reset.", serverId);
    }

    public void setActive(boolean active) {
        messageReceiver.setActive(active);
        server.setActive(active);
    }

    public void start() {
        logger.info("Starting Server {}", serverId);
        try {
            executorManager.submitListeningTask(() -> messageReceiver.startListening(server::warmup));
        } catch (Exception e) {
            logger.error("Server {} encountered an error: {}", serverId, e.getMessage(), e);
        }
    }

    public String getDB() {
        Set<Integer> modifiedAccounts = server.getModifiedAccounts();
        logger.info("Modified accounts for Server {}: {}", serverId, modifiedAccounts);
        StringBuilder sb = new StringBuilder();
        if (modifiedAccounts.isEmpty()) {
//            sb.append("No accounts have been modified. Printing balances of first 10 accounts\n");
            for (int accountId = 1; accountId <= 10; accountId++) {
                int key = (Config.getDatabaseSize() / Config.getServerClusterCount()) * ((serverId - 1) / Config.getServerClusterCount()) + accountId;
                Double balance = database.get(key);
                sb.append(key).append(" : ").append(balance).append("; ");
            }
            return sb.toString();
        } else {
//            sb.append("Modified Accounts:\n");
            for (Integer accountId : modifiedAccounts) {
                Double balance = database.get(accountId);
                sb.append(accountId).append(" : ").append(balance).append("; ");
            }
            return sb.toString();
        }
    }

    public void shutdown() {
        logger.info("Shutting down Server {}", serverId);
        messageReceiver.shutdown();
        executorManager.shutdown();
    }

    public static void main(String[] args) {
        System.out.println("Hello, World!");

        if (args.length != 1) {
            System.err.println("Node ID argument required");
            System.exit(1);
        }

        int sid = Integer.parseInt(args[0]);

        // Compute a safe ISO-like timestamp for the log filename (no characters illegal on Windows)
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss.SSS"));
        // Build a log path and expose it as a system property so Log4j can interpolate it without extra property indirection
        String logPath = "logs/server-" + sid + "/log-" + timestamp + ".log";
        System.setProperty("logPath", logPath);

        // Ensure parent directories for the log file exist so the File appender can create the file
        File parent = new File(logPath).getParentFile();
        if (parent != null && !parent.exists()) {
            try {
                Files.createDirectories(parent.toPath());
            } catch (IOException ioe) {
                // Can't rely on Log4j being configured yet; print to stderr as a last resort
                System.err.println("Failed to create log directory '" + parent.getAbsolutePath() + "': " + ioe.getMessage());
            }
        }

        // Reconfigure Log4j context so it picks up the new system properties (logPath)
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.reconfigure();

        // Now it's safe to create the ServerMain instance which in turn creates the instance logger
        Logger tempLogger = LogManager.getLogger(ServerMain.class);
        tempLogger.info("Starting ServerMain for server ID {}", sid);

        Config.initialize();
        ServerMain serverMain = new ServerMain(sid);
        // Add a shutdown hook so shutdown() is called on JVM exit (SIGINT, SIGTERM, etc.)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                serverMain.logger.info("Shutdown hook triggered - shutting down server");
                serverMain.shutdown();
            } catch (Exception e) {
                // Best-effort shutdown logging; avoid throwing from shutdown hook
                Logger hookLogger = LogManager.getLogger(ServerMain.class);
                hookLogger.error("Error during shutdown hook: {}", e.getMessage(), e);
            }
        }));

        serverMain.start();
    }
}