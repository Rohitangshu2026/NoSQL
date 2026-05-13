package com.etl;

import com.etl.core.ETLConfig;
import com.etl.core.Pipeline;
import com.etl.core.PipelineResult;
import com.etl.db.ResultLoader;
import com.etl.report.Reporter;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class ETLRunner {

    private static final Set<String> VALID_PIPELINES =
            new HashSet<>(Arrays.asList("mapreduce", "pig", "mongodb", "hive"));
    private static final Set<String> VALID_QUERIES =
            new HashSet<>(Arrays.asList("daily_traffic", "top_resources", "hourly_errors", "all", "q1", "q2", "q3"));

    public static void main(String[] args) {
        String pipelineName = null;
        String inputPath = null;
        int batchSize = 50000;
        String dbUrl = null;
        String dbUser = null;
        String dbPass = null;
        String query = "all";
        String mongoUri = "mongodb://localhost:27017";
        String mongoDb  = "etl_logs";
        String hiveUrl  = "jdbc:hive2://localhost:10000/default";
        String hiveUser = "";
        String hivePass = "";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--pipeline":   pipelineName = args[++i]; break;
                case "--input":      inputPath    = args[++i]; break;
                case "--batch-size": batchSize    = Integer.parseInt(args[++i]); break;
                case "--db-url":     dbUrl        = args[++i]; break;
                case "--db-user":    dbUser       = args[++i]; break;
                case "--db-pass":    dbPass       = args[++i]; break;
                case "--query":      query        = args[++i]; break;
                case "--mongo-uri":  mongoUri     = args[++i]; break;
                case "--mongo-db":   mongoDb      = args[++i]; break;
                case "--hive-url":   hiveUrl      = args[++i]; break;
                case "--hive-user":  hiveUser     = args[++i]; break;
                case "--hive-pass":  hivePass     = args[++i]; break;
                case "--help":
                case "-h":
                    printUsage();
                    System.exit(0);
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    printUsage();
                    System.exit(1);
            }
        }

        if (pipelineName == null) pipelineName = promptChoice(
                "Select pipeline", new String[]{"mapreduce", "pig", "mongodb", "hive"});

        if (pipelineName == null || inputPath == null || dbUrl == null || dbUser == null || dbPass == null) {
            printUsage();
            System.exit(1);
        }

        pipelineName = pipelineName.toLowerCase();
        if (!VALID_PIPELINES.contains(pipelineName)) {
            System.err.println("Invalid pipeline: " + pipelineName + ". Valid: " + VALID_PIPELINES);
            System.exit(1);
        }

        query = normalizeQuery(query);
        if (!VALID_QUERIES.contains(query)) {
            System.err.println("Invalid query: " + query + ". Valid: daily_traffic|top_resources|hourly_errors|all (or q1/q2/q3)");
            System.exit(1);
        }
        query = normalizeQuery(query);

        UUID runId = UUID.randomUUID();
        ETLConfig config = new ETLConfig(
                pipelineName, inputPath, batchSize, runId,
                dbUrl, dbUser, dbPass,
                query,
                mongoUri, mongoDb,
                hiveUrl, hiveUser, hivePass);

        System.out.println("=================================================");
        System.out.println("Starting ETL run");
        System.out.println("  Pipeline   : " + pipelineName);
        System.out.println("  Query      : " + query);
        System.out.println("  Input      : " + inputPath);
        System.out.println("  Batch size : " + batchSize);
        System.out.println("  Run ID     : " + runId);
        System.out.println("=================================================");

        try {
            long overallStartMs = System.currentTimeMillis();
            Pipeline pipeline = PipelineFactory.create(pipelineName);
            PipelineResult result = pipeline.execute(config);

            ResultLoader.load(config, result);
            Instant overallFinishedAt = Instant.now();
            long overallRuntimeMs = System.currentTimeMillis() - overallStartMs;
            ResultLoader.updateExecutionMetadata(config, Timestamp.from(overallFinishedAt), overallRuntimeMs);
            Reporter.printReport(config, result);

        } catch (UnsupportedOperationException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String normalizeQuery(String q) {
        if (q == null) return "all";
        switch (q.toLowerCase()) {
            case "q1": return "daily_traffic";
            case "q2": return "top_resources";
            case "q3": return "hourly_errors";
            default:   return q.toLowerCase();
        }
    }

    private static String promptChoice(String label, String[] options) {
        if (System.console() == null) return null;
        System.out.println(label + ":");
        for (int i = 0; i < options.length; i++) {
            System.out.println("  " + (i + 1) + ") " + options[i]);
        }
        String line = System.console().readLine("Enter choice [1-" + options.length + "]: ");
        try {
            int idx = Integer.parseInt(line.trim()) - 1;
            if (idx >= 0 && idx < options.length) return options[idx];
        } catch (NumberFormatException ignored) {}
        return null;
    }

    private static void printUsage() {
        System.err.println(
                "Usage:\n" +
                "  ETLRunner --pipeline <mapreduce|pig|mongodb|hive>\n" +
                "            --input <path>\n" +
                "            --batch-size <n>           (default 50000)\n" +
                "            --query <q1|q2|q3|all>     (default all)\n" +
                "            --db-url   <jdbc-url>      (required, MySQL)\n" +
                "            --db-user  <user>          (required, MySQL)\n" +
                "            --db-pass  <pass>          (required, MySQL)\n" +
                "            --mongo-uri <uri>          (default mongodb://localhost:27017)\n" +
                "            --mongo-db  <db>           (default etl_logs)\n" +
                "            --hive-url  <jdbc-url>     (default jdbc:hive2://localhost:10000/default)\n" +
                "            --hive-user <user>         (optional)\n" +
                "            --hive-pass <pass>         (optional)\n");
    }
}
