package org.example;

import org.example.client.TransactionParser;

import java.util.*;

/**
 * Computes a new shard mapping from recent transaction history by approximating
 * a balanced k-way graph partitioning that minimizes cross-shard edges.
 * Vertices = account IDs, edges = undirected edges between accounts that
 * co-occur in a transaction, weighted by frequency.
 */
public class ReshardingPlanner {

    public record Move(int accountId, int oldCluster, int newCluster) {
        @Override
        public String toString() {
            return "(" + accountId + ", " + oldCluster + ", " + newCluster + ")";
        }
    }

//    /**
//     * Compute resharding moves from recent transaction history.
//     *
//     * @param currentAssignment accountId -> current cluster index
//     * @param transactions      recent transactions ("src,dst,amount")
//     * @param clusterCount      number of clusters (e.g. 3)
//     * @return list of moves (account, oldCluster, newCluster) for accounts whose cluster changed
//     */
//    public static List<Move> computeReshardingMoves(Map<Integer, Integer> currentAssignment,
//                                                    List<String> transactions,
//                                                    int clusterCount) {
//        if (clusterCount <= 0) {
//            throw new IllegalArgumentException("clusterCount must be > 0");
//        }
//
//        // 1. Build weighted adjacency graph from transaction history
//        Map<Integer, Map<Integer, Integer>> adjacency = buildAdjacency(transactions);
//
//        // Ensure we include accounts that may appear only in currentAssignment but not in txs
//        for (Integer accountId : currentAssignment.keySet()) {
//            adjacency.computeIfAbsent(accountId, k -> new HashMap<>());
//        }
//
//        // 2. Start from current assignment, fill in any missing accounts
//        Map<Integer, Integer> newAssignment = new HashMap<>(currentAssignment);
//        Map<Integer, Integer> clusterSizes = new HashMap<>();
//        for (int c = 0; c < clusterCount; c++) {
//            clusterSizes.put(c, 0);
//        }
//
//        for (Map.Entry<Integer, Integer> e : newAssignment.entrySet()) {
//            int cluster = e.getValue();
//            clusterSizes.put(cluster, clusterSizes.getOrDefault(cluster, 0) + 1);
//        }
//
//        // Assign any accounts without an initial cluster to the smallest cluster
//        for (Integer accountId : adjacency.keySet()) {
//            if (!newAssignment.containsKey(accountId)) {
//                int smallestCluster = findSmallestCluster(clusterSizes, clusterCount);
//                newAssignment.put(accountId, smallestCluster);
//                clusterSizes.put(smallestCluster, clusterSizes.get(smallestCluster) + 1);
//            }
//        }
//
//        int vertexCount = adjacency.size();
//        int targetPerCluster = (int) Math.ceil(vertexCount / (double) clusterCount);
//        int maxPasses = 5;
//        Random rnd = new Random();
//
//        List<Integer> vertices = new ArrayList<>(adjacency.keySet());
//        for (int pass = 0; pass < maxPasses; pass++) {
//            boolean movedAny = false;
//            Collections.shuffle(vertices, rnd);
//
//            for (Integer v : vertices) {
//                int currentCluster = newAssignment.get(v);
//                int currentCost = costForVertex(v, currentCluster, newAssignment, adjacency);
//
//                int bestCluster = currentCluster;
//                int bestDelta = 0;
//
//                for (int c = 0; c < clusterCount; c++) {
//                    if (c == currentCluster) continue;
//
//                    if (clusterSizes.get(c) >= targetPerCluster) {
//                        continue;
//                    }
//
//                    int newCost = costForVertex(v, c, newAssignment, adjacency);
//                    int delta = newCost - currentCost; // negative is good (improvement)
//
//                    if (delta < bestDelta) {
//                        bestDelta = delta;
//                        bestCluster = c;
//                    }
//                }
//
//                if (bestCluster != currentCluster) {
//                    // Apply move
//                    newAssignment.put(v, bestCluster);
//                    clusterSizes.put(currentCluster, clusterSizes.get(currentCluster) - 1);
//                    clusterSizes.put(bestCluster, clusterSizes.get(bestCluster) + 1);
//                    movedAny = true;
//                }
//            }
//
//            if (!movedAny) {
//                break;
//            }
//        }
//
//        // 4. Build list of (account, oldCluster, newCluster) moves
//        List<Move> moves = new ArrayList<>();
//        for (Map.Entry<Integer, Integer> e : newAssignment.entrySet()) {
//            int accountId = e.getKey();
//            int newCluster = e.getValue();
//            int oldCluster = currentAssignment.getOrDefault(accountId, -1);
//
//            if (oldCluster != newCluster) {
//                moves.add(new Move(accountId, oldCluster, newCluster));
//            }
//        }
//        return moves;
//    }

    public static List<Move> computeReshardingMoves(Map<Integer, Integer> currentAssignment,
                                                    List<String> transactions,
                                                    int clusterCount) {
        if (clusterCount <= 0) {
            throw new IllegalArgumentException("clusterCount must be > 0");
        }

        Map<Integer, Map<Integer, Integer>> adjacency = buildAdjacency(transactions);

        // Ensure all known accounts appear as vertices
        for (Integer accountId : currentAssignment.keySet()) {
            adjacency.computeIfAbsent(accountId, k -> new HashMap<>());
        }

        Map<Integer, Integer> newAssignment = new HashMap<>(currentAssignment);

        // Optional: keep your existing single-vertex improvement here,
        // but you can also skip it and rely only on swaps.

        improveWithSwaps(newAssignment, adjacency, clusterCount, 5);

        List<Move> moves = new ArrayList<>();
        for (Map.Entry<Integer, Integer> e : newAssignment.entrySet()) {
            int accountId  = e.getKey();
            int newCluster = e.getValue();
            int oldCluster = currentAssignment.getOrDefault(accountId, -1);
            if (oldCluster != newCluster) {
                moves.add(new Move(accountId, oldCluster, newCluster));
            }
        }
        return moves;
    }

    private static void improveWithSwaps(Map<Integer, Integer> assignment,
                                         Map<Integer, Map<Integer, Integer>> adj,
                                         int clusterCount,
                                         int maxPasses) {

        List<Integer> active = new ArrayList<>();
        for (Map.Entry<Integer, Map<Integer, Integer>> e : adj.entrySet()) {
            if (!e.getValue().isEmpty()) {
                active.add(e.getKey());
            }
        }
        if (active.isEmpty()) return;

        Random rnd = new Random();

        for (int pass = 0; pass < maxPasses; pass++) {
            boolean improved = false;
            Collections.shuffle(active, rnd);

            int bestU = -1, bestV = -1;
            int bestDelta = 0;

            for (int i = 0; i < active.size(); i++) {
                int u = active.get(i);
                int cu = assignment.get(u);

                // Pair u only with vertices in *other* clusters.
                for (Map.Entry<Integer, Integer> e : assignment.entrySet()) {
                    int v = e.getKey();
                    int cv = e.getValue();
                    if (cv == cu) continue;
                    // Optional: skip cold–cold swaps
                    if (adj.getOrDefault(v, Map.of()).isEmpty() &&
                            adj.getOrDefault(u, Map.of()).isEmpty()) {
                        continue;
                    }

                    int delta = swapGain(u, v, assignment, adj);
                    if (delta < bestDelta) {
                        bestDelta = delta;
                        bestU = u;
                        bestV = v;
                    }
                }
            }

            if (bestDelta < 0) {
                int cu = assignment.get(bestU);
                int cv = assignment.get(bestV);
                assignment.put(bestU, cv);
                assignment.put(bestV, cu);
                improved = true;
            }

            if (!improved) break;
        }
    }


    /**
     * Gain (delta in cut cost) from swapping u and v.
     * Negative is good (reduces cross-shard cost).
     */
    private static int swapGain(int u, int v,
                                Map<Integer, Integer> assignment,
                                Map<Integer, Map<Integer, Integer>> adj) {
        int cu = assignment.get(u);
        int cv = assignment.get(v);

        int before =
                costForVertex(u, cu, assignment, adj)
                        + costForVertex(v, cv, assignment, adj);

        // Temporarily apply swap
        assignment.put(u, cv);
        assignment.put(v, cu);

        int after =
                costForVertex(u, cv, assignment, adj)
                        + costForVertex(v, cu, assignment, adj);

        // Revert
        assignment.put(u, cu);
        assignment.put(v, cv);

        return after - before;
    }


    private static Map<Integer, Map<Integer, Integer>> buildAdjacency(List<String> transactions) {
        Map<Integer, Map<Integer, Integer>> adj = new HashMap<>();

        if (transactions == null) return adj;

        for (String tx : transactions) {
            if (tx == null) continue;

            // Quick check for CLI commands (F/R) using TransactionParser.firstLetter
            char first = TransactionParser.firstLetter(tx);
            if (first == 'F' || first == 'f' || first == 'R' || first == 'r') {
                continue; // not a client transaction
            }

            // Normalize and strip outer parens/brackets
            String content = TransactionParser.stripOuterParens(tx);
            if (content.isEmpty()) continue;

            // Split and trim parts; ClientNode treats 2+ fields as transfer-like, but we only
            // want to handle the transfer case (at least two numeric fields: sender,receiver)
            String[] parts = TransactionParser.splitAndTrim(content);
            if (parts.length < 2) {
                // single-value balance request or malformed -> skip
                continue;
            }

            // Try parse first two fields as integers (sender, receiver)
            int a, b;
            try {
                a = Integer.parseInt(parts[0]);
                b = Integer.parseInt(parts[1]);
            } catch (NumberFormatException nfe) {
                // Not a transfer with numeric account ids -> skip
                continue;
            }

            // Ensure vertices exist
            adj.computeIfAbsent(a, k -> new HashMap<>());
            adj.computeIfAbsent(b, k -> new HashMap<>());

            if (a == b) continue; // ignore self-transfers for adjacency

            // Undirected increment
            System.out.println("ReshardingPlanner: adding edge " + a + " <-> " + b + " from tx: '" + tx + "'");
            incrementEdge(adj, a, b);
            incrementEdge(adj, b, a);
        }

        return adj;
    }

    private static void incrementEdge(Map<Integer, Map<Integer, Integer>> adj,
                                      int from, int to) {
        Map<Integer, Integer> nbrs =
                adj.computeIfAbsent(from, k -> new HashMap<>());
        nbrs.put(to, nbrs.getOrDefault(to, 0) + 1);
    }

    /**
     * Cost contribution for one vertex if assigned to 'cluster':
     * sum of weights of edges from v to neighbors in *other* clusters.
     */
    private static int costForVertex(int v,
                                     int cluster,
                                     Map<Integer, Integer> assignment,
                                     Map<Integer, Map<Integer, Integer>> adj) {
        Map<Integer, Integer> nbrs = adj.get(v);
        if (nbrs == null) {
            return 0;
        }

        int cost = 0;
        for (Map.Entry<Integer, Integer> e : nbrs.entrySet()) {
            int neighbor = e.getKey();
            int weight = e.getValue();
            int neighborCluster = assignment.getOrDefault(neighbor, cluster);
            if (neighborCluster != cluster) {
                cost += weight;
            }
        }
        return cost;
    }

    private static int findSmallestCluster(Map<Integer, Integer> clusterSizes, int clusterCount) {
        int bestCluster = 0;
        int bestSize = Integer.MAX_VALUE;
        for (int c = 0; c < clusterCount; c++) {
            int size = clusterSizes.getOrDefault(c, 0);
            if (size < bestSize) {
                bestSize = size;
                bestCluster = c;
            }
        }
        return bestCluster;
    }
}
