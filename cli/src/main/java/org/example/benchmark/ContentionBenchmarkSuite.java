package org.example.benchmark;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Generates datasets with varying contention levels and runs multiple benchmark iterations,
 * storing results for analysis.
 */
public class ContentionBenchmarkSuite {
    private static final Logger logger = LogManager.getLogger(ContentionBenchmarkSuite.class);

    private static final int RUNS_PER_DATASET = 3;
    private static final double[] SKEW_LEVELS = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};

    private final BenchmarkExecutor executor;
    private final Map<Integer, Integer> accountToClusterIndex;
    private final List<BenchmarkResult> results = new ArrayList<>();

    /**
     * Functional interface for executing a single benchmark run.
     */
    @FunctionalInterface
    public interface BenchmarkExecutor {
        /**
         * Execute benchmark with given transactions.
         *
         * @param transactions list of CSV-formatted transactions
         * @param skew         the skew value used to generate this dataset
         * @return the benchmark result
         */
        BenchmarkResult execute(List<String> transactions, double skew);
    }

    /**
     * Holds results from a single benchmark run.
     */
    public static class BenchmarkResult {
        public final double skew;
        public final int contentionPercent;
        public final int runNumber;
        public final int totalRequests;
        public final int completedRequests;
        public final double elapsedSeconds;
        public final double throughput;
        public final double avgLatencyMs;
        public final long minLatencyMs;
        public final long maxLatencyMs;

        public BenchmarkResult(double skew, int runNumber, int totalRequests, int completedRequests,
                               double elapsedSeconds, double throughput,
                               double avgLatencyMs, long minLatencyMs, long maxLatencyMs) {
            this.skew = skew;
            this.contentionPercent = (int) Math.round(skew * 100);
            this.runNumber = runNumber;
            this.totalRequests = totalRequests;
            this.completedRequests = completedRequests;
            this.elapsedSeconds = elapsedSeconds;
            this.throughput = throughput;
            this.avgLatencyMs = avgLatencyMs;
            this.minLatencyMs = minLatencyMs;
            this.maxLatencyMs = maxLatencyMs;
        }

        public String toCsvRow() {
            return String.format(Locale.US, "%d,%d,%d,%d,%.3f,%.2f,%.2f,%d,%d",
                    contentionPercent, runNumber, totalRequests, completedRequests,
                    elapsedSeconds, throughput, avgLatencyMs, minLatencyMs, maxLatencyMs);
        }
    }

    public ContentionBenchmarkSuite(BenchmarkExecutor executor, Map<Integer, Integer> accountToClusterIndex) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.accountToClusterIndex = Objects.requireNonNull(accountToClusterIndex, "accountToClusterIndex");
    }

    /**
     * Run the full benchmark suite: 10 contention levels × 3 runs each.
     *
     * @param transactionsPerDataset number of transactions per dataset
     */
    public void runSuite(int transactionsPerDataset) {
        logger.info("Starting contention benchmark suite: {} transactions, {} skew levels, {} runs each",
                transactionsPerDataset, SKEW_LEVELS.length, RUNS_PER_DATASET);

        // Pre-generate all datasets
        Map<Double, List<String>> datasets = generateDatasets(transactionsPerDataset);

        for (double skew : SKEW_LEVELS) {
            List<String> transactions = datasets.get(skew);
            int contentionPercent = (int) Math.round(skew * 100);

            logger.info("Running benchmarks for {}% contention (skew={})", contentionPercent, skew);

            for (int run = 1; run <= RUNS_PER_DATASET; run++) {
                logger.info("  Run {}/{}", run, RUNS_PER_DATASET);

                BenchmarkResult result = executor.execute(transactions, skew);

                // Create result with correct run number
                BenchmarkResult indexedResult = new BenchmarkResult(
                        skew, run, result.totalRequests, result.completedRequests,
                        result.elapsedSeconds, result.throughput,
                        result.avgLatencyMs, result.minLatencyMs, result.maxLatencyMs
                );
                results.add(indexedResult);

                logger.info("    Completed: throughput={:.2f} req/s, avgLatency={:.2f} ms",
                        result.throughput, result.avgLatencyMs);

                // Brief pause between runs
                sleep(2000);
            }
        }

        logger.info("Benchmark suite complete. Total runs: {}", results.size());
    }

    /**
     * Generate datasets for all skew levels.
     */
    private Map<Double, List<String>> generateDatasets(int transactionsPerDataset) {
        Map<Double, List<String>> datasets = new LinkedHashMap<>();

        for (double skew : SKEW_LEVELS) {
            List<String> txs = ClientBenchmark.buildIntraShardTransfersWithSkew(
                    accountToClusterIndex, transactionsPerDataset, skew);
            datasets.put(skew, txs);
            logger.info("Generated dataset for skew={} ({}% contention): {} transactions",
                    skew, (int) (skew * 100), txs.size());
        }

        return datasets;
    }

    /**
     * Get all collected results.
     */
    public List<BenchmarkResult> getResults() {
        return Collections.unmodifiableList(results);
    }

    /**
     * Export results to CSV file.
     */
    public void exportResultsToCsv(String outputPath) {
        List<String> lines = new ArrayList<>();
        lines.add("contention_percent,run,total_requests,completed_requests,elapsed_seconds,throughput,avg_latency_ms,min_latency_ms,max_latency_ms");

        for (BenchmarkResult result : results) {
            lines.add(result.toCsvRow());
        }

        Path path = Paths.get(outputPath);
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(path, lines, StandardCharsets.UTF_8);
            logger.info("Exported {} results to {}", results.size(), path.toAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to export results: " + e.getMessage(), e);
        }
    }

    /**
     * Export results with timestamp in filename.
     */
    public void exportResults() {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        exportResultsToCsv("benchmark_results_" + timestamp + ".csv");
    }

    /**
     * Generate and save the throughput chart.
     * Shows individual run dots, average line per contention level, and overall average.
     *
     * @param outputPath path to save the PNG image
     */
    public void generateThroughputChart(String outputPath) {
        XYSeriesCollection dataset = new XYSeriesCollection();

        // Calculate averages per contention level
        Map<Integer, List<BenchmarkResult>> byContention = new LinkedHashMap<>();
        for (BenchmarkResult r : results) {
            byContention.computeIfAbsent(r.contentionPercent, k -> new ArrayList<>()).add(r);
        }

        // Series for individual run dots (no lines)
        XYSeries dotsSeries = new XYSeries("Individual Runs");
        for (BenchmarkResult r : results) {
            dotsSeries.add(r.contentionPercent, r.throughput);
        }
        dataset.addSeries(dotsSeries);

        // Series for average throughput line
        XYSeries avgSeries = new XYSeries("Average Throughput");
        double totalThroughput = 0;
        int count = 0;
        for (Map.Entry<Integer, List<BenchmarkResult>> entry : byContention.entrySet()) {
            int contention = entry.getKey();
            List<BenchmarkResult> runs = entry.getValue();
            double avg = runs.stream().mapToDouble(r -> r.throughput).average().orElse(0);
            avgSeries.add(contention, avg);
            totalThroughput += runs.stream().mapToDouble(r -> r.throughput).sum();
            count += runs.size();
        }
        dataset.addSeries(avgSeries);

        // Calculate overall average
        double overallAvg = count > 0 ? totalThroughput / count : 0;

        // Series for overall average horizontal line
        XYSeries overallAvgSeries = new XYSeries("Overall Average (" + String.format("%.1f", overallAvg) + " req/s)");
        overallAvgSeries.add(10, overallAvg);
        overallAvgSeries.add(100, overallAvg);
        dataset.addSeries(overallAvgSeries);

        JFreeChart chart = ChartFactory.createXYLineChart(
                "Throughput vs Contention Level",
                "Contention Level (%)",
                "Throughput (requests/second)",
                dataset,
                PlotOrientation.VERTICAL,
                true,   // legend
                true,   // tooltips
                false   // urls
        );

        customizeThroughputChart(chart, overallAvg);
        saveChart(chart, outputPath, 800, 600);
    }

    /**
     * Customize the throughput chart appearance.
     */
    private void customizeThroughputChart(JFreeChart chart, double overallAvg) {
        chart.setBackgroundPaint(Color.WHITE);

        XYPlot plot = chart.getXYPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.LIGHT_GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

        // Configure X-axis
        NumberAxis domainAxis = (NumberAxis) plot.getDomainAxis();
        domainAxis.setRange(5, 105);
        domainAxis.setTickUnit(new org.jfree.chart.axis.NumberTickUnit(10));

        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();

        // Series 0: Individual runs - dots only, no lines
        renderer.setSeriesLinesVisible(0, false);
        renderer.setSeriesShapesVisible(0, true);
        renderer.setSeriesShape(0, new java.awt.geom.Ellipse2D.Double(-5, -5, 10, 10));
        renderer.setSeriesPaint(0, new Color(31, 119, 180, 180)); // Blue with some transparency

        // Series 1: Average throughput - line with small dots
        renderer.setSeriesLinesVisible(1, true);
        renderer.setSeriesShapesVisible(1, true);
        renderer.setSeriesShape(1, new java.awt.geom.Ellipse2D.Double(-3, -3, 6, 6));
        renderer.setSeriesPaint(1, new Color(214, 39, 40)); // Red
        renderer.setSeriesStroke(1, new BasicStroke(2.5f));

        // Series 2: Overall average - horizontal dashed grey line
        renderer.setSeriesLinesVisible(2, true);
        renderer.setSeriesShapesVisible(2, false);
        renderer.setSeriesPaint(2, Color.GRAY);
        renderer.setSeriesStroke(2, new BasicStroke(2.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND,
                1.0f, new float[]{10.0f, 6.0f}, 0.0f));

        plot.setRenderer(renderer);
    }


    /**
     * Generate and save the latency chart showing avg, min, and max latencies.
     *
     * @param outputPath path to save the PNG image
     */
    public void generateLatencyChart(String outputPath) {
        XYSeriesCollection dataset = new XYSeriesCollection();

        // Calculate average of the 3 runs for each contention level
        Map<Integer, List<BenchmarkResult>> byContention = new LinkedHashMap<>();
        for (BenchmarkResult r : results) {
            byContention.computeIfAbsent(r.contentionPercent, k -> new ArrayList<>()).add(r);
        }

        XYSeries avgSeries = new XYSeries("Avg Latency");
        XYSeries minSeries = new XYSeries("Min Latency");
        XYSeries maxSeries = new XYSeries("Max Latency");

        for (Map.Entry<Integer, List<BenchmarkResult>> entry : byContention.entrySet()) {
            int contention = entry.getKey();
            List<BenchmarkResult> runs = entry.getValue();

            double avgOfAvgs = runs.stream().mapToDouble(r -> r.avgLatencyMs).average().orElse(0);
            long minOfMins = runs.stream().mapToLong(r -> r.minLatencyMs).min().orElse(0);
            long maxOfMaxs = runs.stream().mapToLong(r -> r.maxLatencyMs).max().orElse(0);

            avgSeries.add(contention, avgOfAvgs);
            minSeries.add(contention, minOfMins);
            maxSeries.add(contention, maxOfMaxs);
        }

        dataset.addSeries(avgSeries);
        dataset.addSeries(minSeries);
        dataset.addSeries(maxSeries);

        JFreeChart chart = ChartFactory.createXYLineChart(
                "Latency vs Contention Level",
                "Contention Level (%)",
                "Latency (ms)",
                dataset,
                PlotOrientation.VERTICAL,
                true,   // legend
                true,   // tooltips
                false   // urls
        );

        customizeLatencyChart(chart);
        saveChart(chart, outputPath, 800, 600);
    }

    /**
     * Customize the throughput chart appearance.
     */
    private void customizeChart(JFreeChart chart, int seriesCount) {
        chart.setBackgroundPaint(Color.WHITE);

        XYPlot plot = chart.getXYPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.LIGHT_GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

        // Configure X-axis
        NumberAxis domainAxis = (NumberAxis) plot.getDomainAxis();
        domainAxis.setRange(5, 105);
        domainAxis.setTickUnit(new org.jfree.chart.axis.NumberTickUnit(10));

        // Configure renderer to show dots
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        Color[] colors = {new Color(31, 119, 180), new Color(255, 127, 14), new Color(44, 160, 44)};

        for (int i = 0; i < seriesCount; i++) {
            renderer.setSeriesLinesVisible(i, true);
            renderer.setSeriesShapesVisible(i, true);
            renderer.setSeriesShape(i, new java.awt.geom.Ellipse2D.Double(-4, -4, 8, 8));
            renderer.setSeriesPaint(i, colors[i % colors.length]);
            renderer.setSeriesStroke(i, new BasicStroke(2.0f));
        }

        plot.setRenderer(renderer);
    }

    /**
     * Customize the latency chart appearance.
     */
    private void customizeLatencyChart(JFreeChart chart) {
        chart.setBackgroundPaint(Color.WHITE);

        XYPlot plot = chart.getXYPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.LIGHT_GRAY);
        plot.setRangeGridlinePaint(Color.LIGHT_GRAY);

        // Configure X-axis
        NumberAxis domainAxis = (NumberAxis) plot.getDomainAxis();
        domainAxis.setRange(5, 105);
        domainAxis.setTickUnit(new org.jfree.chart.axis.NumberTickUnit(10));

        // Configure renderer
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();

        // Avg - solid blue line with circles
        renderer.setSeriesPaint(0, new Color(31, 119, 180));
        renderer.setSeriesStroke(0, new BasicStroke(2.5f));
        renderer.setSeriesShape(0, new java.awt.geom.Ellipse2D.Double(-5, -5, 10, 10));
        renderer.setSeriesShapesVisible(0, true);

        // Min - dashed green line with triangles
        renderer.setSeriesPaint(1, new Color(44, 160, 44));
        renderer.setSeriesStroke(1, new BasicStroke(2.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND,
                1.0f, new float[]{6.0f, 4.0f}, 0.0f));
        renderer.setSeriesShape(1, createTriangle(6));
        renderer.setSeriesShapesVisible(1, true);

        // Max - dashed red line with squares
        renderer.setSeriesPaint(2, new Color(214, 39, 40));
        renderer.setSeriesStroke(2, new BasicStroke(2.0f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND,
                1.0f, new float[]{6.0f, 4.0f}, 0.0f));
        renderer.setSeriesShape(2, new java.awt.geom.Rectangle2D.Double(-4, -4, 8, 8));
        renderer.setSeriesShapesVisible(2, true);

        plot.setRenderer(renderer);
    }

    /**
     * Create a triangle shape for chart markers.
     */
    private Shape createTriangle(double size) {
        java.awt.geom.Path2D.Double path = new java.awt.geom.Path2D.Double();
        path.moveTo(0, -size);
        path.lineTo(size, size);
        path.lineTo(-size, size);
        path.closePath();
        return path;
    }

    /**
     * Save chart to file.
     */
    private void saveChart(JFreeChart chart, String outputPath, int width, int height) {
        try {
            File file = new File(outputPath);
            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
            ChartUtils.saveChartAsPNG(file, chart, width, height);
            logger.info("Saved chart to {}", file.getAbsolutePath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to save chart: " + e.getMessage(), e);
        }
    }

    /**
     * Generate all charts with timestamped filenames.
     */
    public void generateCharts() {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        generateThroughputChart("throughput_chart_" + timestamp + ".png");
        generateLatencyChart("latency_chart_" + timestamp + ".png");
    }

    /**
     * Print summary table to console.
     */
    public void printSummary() {
        System.out.println("\n=== Contention Benchmark Summary ===\n");
        System.out.println("Contention% | Run | Throughput (req/s) | Avg Latency (ms) | Min | Max");
        System.out.println("------------|-----|--------------------|--------------------|-----|-----");

        for (BenchmarkResult r : results) {
            System.out.printf("%11d | %3d | %18.2f | %18.2f | %3d | %3d%n",
                    r.contentionPercent, r.runNumber, r.throughput, r.avgLatencyMs,
                    r.minLatencyMs, r.maxLatencyMs);
        }
        System.out.println();
    }

    private void sleep(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
