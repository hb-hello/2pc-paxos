package org.example.client;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.config.Config;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TransactionSetLoader {

    private static final Logger logger = LogManager.getLogger(TransactionSetLoader.class);

    public List<TransactionSet> loadAll() throws IOException {
        String path = Config.getTransactionsSetsPath();
        return loadAll(Path.of(path));
    }

    public List<TransactionSet> loadAll(Path csvPath) throws IOException {
        List<TransactionSet> result = new ArrayList<>();

        try (Reader reader = Files.newBufferedReader(csvPath);
             CSVReader csvReader = new CSVReaderBuilder(reader)
                     .withSkipLines(1)  // skip header row[web:5]
                     .build()) {

            int currentSetNumber = -1;
            List<Integer> currentLiveNodes = List.of();
            List<String> currentTransactions = new ArrayList<>();

            while (true) {
                String[] cols;
                try {
                    cols = csvReader.readNext(); // may throw CsvValidationException[web:16]
                } catch (CsvValidationException e) {
                    System.err.println("Skipping invalid CSV line: " + e.getMessage());
                    continue;
                }

                if (cols == null) {
                    break; // EOF
                }
                if (cols.length == 0) {
                    continue;
                }

                String setNumCol = safeGet(cols, 0).trim();
                String txCol     = safeGet(cols, 1).trim();
                String liveCol   = safeGet(cols, 2).trim();

                if (!setNumCol.isEmpty()) {
                    // Starting a new set: flush previous if any
                    if (currentSetNumber != -1) {
                        result.add(new TransactionSet(
                                currentSetNumber,
                                currentLiveNodes,
                                currentTransactions
                        ));
                    }

                    currentSetNumber = Integer.parseInt(setNumCol);
                    currentTransactions = new ArrayList<>();
                    currentLiveNodes = List.of(); // will be set if/when liveCol is present
                }

                if (currentSetNumber == -1) {
                    throw new IllegalStateException("Found transaction row before any set number");
                }

                if (!liveCol.isEmpty()) {
                    currentLiveNodes = parseLiveNodes(liveCol);
                }

                if (!txCol.isEmpty()) {
                    // Preserve content but normalize whitespace at edges
                    String tx = txCol.trim();
                    if (!tx.isEmpty()) {
                        currentTransactions.add(tx);
                    }
                }
            }

            // Flush last set
            if (currentSetNumber != -1) {
                result.add(new TransactionSet(
                        currentSetNumber,
                        currentLiveNodes,
                        currentTransactions
                ));
            }
        }

        return result;
    }

    private static String safeGet(String[] arr, int idx) {
        return idx < arr.length ? arr[idx] : "";
    }

    /**
     * Accepts formats like:
     *   - n1, n2, n3
     *   - [n1, n2, n3, n4]
     */
    private static List<Integer> parseLiveNodes(String liveCol) {
        String s = liveCol.trim();
        if (s.startsWith("[") && s.endsWith("]")) {
            s = s.substring(1, s.length() - 1);
        }
        if (s.isEmpty()) {
            return List.of();
        }
        String[] parts = s.split("\\s*,\\s*");
        List<String> nodes = new ArrayList<>(parts.length);
        for (String p : parts) {
            if (!p.isEmpty()) {
                nodes.add(p.trim());
            }
        }

        List<Integer> ids = new ArrayList<>();

        for (String node : nodes) {
            if (node == null) continue;
            String str = node.trim();
            if (str.isEmpty()) continue;
            if (str.startsWith("[") && str.endsWith("]")) {
                str = str.substring(1, str.length() - 1).trim();
            }
            if (str.startsWith("n") || str.startsWith("N")) {
                str = str.substring(1);
            }
            try {
                ids.add(Integer.parseInt(str));
            } catch (NumberFormatException e) {
                logger.warn("Warning: unable to parse live node '" + node + "'");
            }
        }

        return ids;
    }
}
