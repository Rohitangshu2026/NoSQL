package com.etl;

import com.etl.core.ETLConfig;
import com.etl.core.Pipeline;
import com.etl.core.PipelineResult;
import com.etl.db.ResultLoader;
import com.etl.report.Reporter;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

public class ETLRunner {

    public static void main(String[] args) {
        String pipelineName = "mapreduce";
        String inputPath = null;
        int batchSize = 1000;
        String dbUrl = null;
        String dbUser = null;
        String dbPass = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--pipeline":
                    pipelineName = args[++i];
                    break;
                case "--input":
                    inputPath = args[++i];
                    break;
                case "--batch-size":
                    batchSize = Integer.parseInt(args[++i]);
                    break;
                case "--db-url":
                    dbUrl = args[++i];
                    break;
                case "--db-user":
                    dbUser = args[++i];
                    break;
                case "--db-pass":
                    dbPass = args[++i];
                    break;
            }
        }

        if (inputPath == null || dbUrl == null || dbUser == null || dbPass == null) {
            System.err.println("Usage: ETLRunner --pipeline <name> --input <path> --batch-size <size> --db-url <url> --db-user <user> --db-pass <pass>");
            System.exit(1);
        }

        UUID runId = UUID.randomUUID();
        ETLConfig config = new ETLConfig(pipelineName, inputPath, batchSize, runId, dbUrl, dbUser, dbPass);

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
}
