package org.example;

import org.example.config.Config;
import org.example.messaging.CLIMessageSender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.example.persistence.DBHandler;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import java.util.concurrent.Executors;

public class CliMain {

    private final DBHandler dbHandler;
    private CLIMessageSender cliMessageSender;

    public CliMain() {
        // Instantiate DBHandler so CLI commands can query DB contents
        this.dbHandler = new DBHandler();

        startAllServers();
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
//            System.out.println("Resetting all servers before client warmup...");
            // Activate all known servers so clients can warm connections to all of them
            System.out.println("Server reset/activation complete.");
        } catch (Exception e) {
            System.out.println("Warning: failed to reset/activate servers before warmup: " + e.getMessage());
        }
    }


    private void startAllServers() {
        try {
            ServerManager.startAllServers(Config.getServerExecutablePath(), Config.getServerCount());

            // this calls the warmup action

            this.cliMessageSender = new CLIMessageSender(0, Executors.newSingleThreadExecutor());

            try {
                Thread.sleep(1000); // wait a bit for processes to start

                //potentially start receiver server
                Thread.sleep(1000); // wait a bit for GRPC servers to start
                cliMessageSender.warmup();

                activateAllServers();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    private void shutdown() {
        try {
            dbHandler.shutdown();
        } catch (Exception ignored) {
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

        int next = 0;
        Scanner sc = new Scanner(System.in);

        while (true) {
            System.out.println();
            System.out.println("Options:");
            System.out.println(" 1 - PrintDB");
            System.out.println(" 2 - PrintLog");
            System.out.println(" 3 - PrintStatus");
            System.out.println(" 4 - PrintView");
            System.out.println(" 5 - Continue with next set (#" + (next + 1) + ")");
            System.out.println(" 6 - DEBUG: PrintOperationLog");
            System.out.println(" 7 - DEBUG: Pause/Resume client (pause a client to inspect logs/db)");
            System.out.println(" 8 - DEBUG: Choose next set number");
            System.out.println(" 9 - DEBUG: PrintDB directly");
            System.out.println(" 0 - Exit");
            System.out.print("Choice: ");
            String choice = sc.nextLine().trim();

            switch (choice) {
                case "1" -> System.out.println("Printing DB contents from all servers:");
                case "9" -> {
                    System.out.print("Enter client ID: ");
                    String idStr = sc.nextLine().trim();
                    cli.printDBForAccountId(idStr);
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