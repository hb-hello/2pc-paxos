package org.example.client;

import java.util.ArrayList;
import java.util.List;

public final class TransactionParser {

    private TransactionParser() {
        // utility class
    }

    /**
     * First alphabetic character in the string, or '\0' if none.
     */
    public static char firstLetter(String s) {
        if (s == null) return '\0';
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isLetter(c)) {
                return c;
            }
        }
        return '\0';
    }

    /**
     * Strip a single pair of outer () or [] if present.
     */
    public static String stripOuterParens(String s) {
        if (s == null) return "";
        String t = s.trim();
        if (t.length() >= 2 &&
                ((t.startsWith("(") && t.endsWith(")")) ||
                        (t.startsWith("[") && t.endsWith("]")))) {
            return t.substring(1, t.length() - 1).trim();
        }
        return t;
    }

    /**
     * Split on comma and trim each part, removing empty segments.
     */
    public static String[] splitAndTrim(String s) {
        if (s == null || s.isBlank()) {
            return new String[0];
        }
        String[] rawParts = s.split(",");
        List<String> parts = new ArrayList<>(rawParts.length);
        for (String p : rawParts) {
            String trimmed = p.trim();
            if (!trimmed.isEmpty()) {
                parts.add(trimmed);
            }
        }
        return parts.toArray(new String[0]);
    }

    /**
     * Parse node id from strings like "F(n3)", "Fn3", "R n10", etc.
     */
    public static int parseNodeId(String line) {
        if (line == null) {
            throw new IllegalArgumentException("Cannot extract node id from null line");
        }
        String clean = line.replaceAll("\\s+", "");
        // Prefer 'n' / 'N' followed by digits
        int idx = clean.indexOf('n');
        if (idx < 0) {
            idx = clean.indexOf('N');
        }
        if (idx >= 0 && idx + 1 < clean.length()) {
            int start = idx + 1;
            int end = start;
            while (end < clean.length() && Character.isDigit(clean.charAt(end))) {
                end++;
            }
            String numStr = clean.substring(start, end);
            return Integer.parseInt(numStr);
        }

        // Fallback: collect all digits
        StringBuilder sb = new StringBuilder();
        for (char c : clean.toCharArray()) {
            if (Character.isDigit(c)) {
                sb.append(c);
            }
        }
        if (sb.length() == 0) {
            throw new IllegalArgumentException("Cannot extract node id from: " + line);
        }
        return Integer.parseInt(sb.toString());
    }
}
