package org.example;

import java.util.Map;

public class CLIFormatter {
    /**
     * Print a consolidated two-column ASCII table for multiple servers.
     * First column: Server ID
     * Second column: Response (may wrap if long or contain newlines)
     */
    public static void printDBAsTable(Map<String, CLIResponse> responses) {
        String col1 = "Server ID";
        String col2 = "Response";

        // Determine column widths
        int width1 = col1.length();
        int maxLineLen = 0; // for response column
        int cap = 120; // cap the response column width to 120 characters

        for (String serverId : responses.keySet()) {
            if (serverId.length() > width1) width1 = serverId.length();
            CLIResponse resp = responses.get(serverId);
            String respStr = resp == null ? "" : resp.getCliResponse();
            String[] lines = respStr.split("\\r?\\n");
            for (String line : lines) {
                if (line.length() > maxLineLen) maxLineLen = line.length();
            }
        }

        int width2 = Math.max(col2.length(), Math.min(maxLineLen, cap));

        // Print header (once)
        printRowBorder(width1, width2);
        System.out.printf("| %s | %s |%n", padRight(col1, width1), padRight(col2, width2));
        printRowBorder(width1, width2);

        // Print each server's response as table rows
        for (String serverId : responses.keySet()) {
            CLIResponse resp = responses.get(serverId);
            String respStr = resp == null ? "" : resp.getCliResponse();
            String[] respLines = respStr.split("\\r?\\n");

            // If response is empty, print a single row with server id and empty response
            if (respLines.length == 0 || (respLines.length == 1 && respLines[0].isEmpty())) {
                System.out.printf("| %s | %s |%n", padRight(serverId, width1), padRight("", width2));
                continue;
            }

            boolean firstRow = true;
            for (String line : respLines) {
                if (line.isEmpty()) {
                    System.out.printf("| %s | %s |%n", padRight(firstRow ? serverId : "", width1), padRight("", width2));
                    firstRow = false;
                    continue;
                }
                int start = 0;
                while (start < line.length()) {
                    int end = Math.min(start + width2, line.length());
                    String chunk = line.substring(start, end);
                    System.out.printf("| %s | %s |%n", padRight(firstRow ? serverId : "", width1), padRight(chunk, width2));
                    start = end;
                    firstRow = false;
                }
            }
        }

        printRowBorder(width1, width2);
        System.out.println();
    }

    private static void printRowBorder(int w1, int w2) {
        System.out.print("+");
        for (int i = 0; i < w1 + 2; i++) System.out.print("-");
        System.out.print("+");
        for (int i = 0; i < w2 + 2; i++) System.out.print("-");
        System.out.println("+");
    }

    private static  String padRight(String s, int n) {
        if (s == null) s = "";
        if (s.length() >= n) return s;
        StringBuilder sb = new StringBuilder(s);
        while (sb.length() < n) sb.append(' ');
        return sb.toString();
    }
}
