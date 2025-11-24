package org.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ServerManager {
    private static final List<Process> processes = new ArrayList<>();

    public static void startAllServers(String jarPath, int numServers) throws IOException {
        for (int i = 1; i <= numServers; i++) {
            // Include the protobuf JVM compatibility flag and the add-opens option when launching child JVMs
            ProcessBuilder pb = new ProcessBuilder(
                    "java",
                    "--add-opens=java.base/sun.misc=ALL-UNNAMED",
                    "-Dcom.google.protobuf.use_unsafe_pre22_gencode=true",
                    "-jar",
                    jarPath,
                    String.valueOf(i)
            );
            pb.inheritIO();
            Process process = pb.start();
            processes.add(process);
            System.out.println("Started server " + i);
        }

        // Register shutdown hook to ensure launched server processes are terminated on JVM exit
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                shutdownAllServers();
            } catch (Exception e) {
                System.err.println("Error shutting down servers in shutdown hook: " + e.getMessage());
            }
        }));
    }

    public static void shutdownAllServers() {
        for (int i = 0; i < processes.size(); i++) {
            Process process = processes.get(i);
            if (process.isAlive()) {
                process.destroy();
                System.out.println("Shut down server " + (i + 1));
            }
        }
        processes.clear();
    }
}
