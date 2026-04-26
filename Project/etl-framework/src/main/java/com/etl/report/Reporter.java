package com.etl.report;

import com.etl.core.ETLConfig;
import com.etl.core.PipelineResult;
import com.etl.core.ResultRow;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Reporter {
    public static void printReport(ETLConfig config, PipelineResult result) {
        System.out.println("=================================================");
        System.out.println("ETL Run Report");
        System.out.println("=================================================");
        System.out.println("Pipeline: " + config.getPipelineName());
        System.out.println("Run ID: " + config.getRunId());
        System.out.println("Runtime: " + result.getRuntimeMs() + " ms (" + (result.getRuntimeMs() / 1000.0) + " seconds)");
        System.out.println("Total Records: " + result.getTotalRecords());
        System.out.println("Malformed Records: " + result.getMalformedCount());
        System.out.println("Total Batches: " + result.getTotalBatches());
        System.out.println("Average Batch Size: " + String.format("%.2f", result.avgBatchSize()));
        System.out.println("=================================================\n");

        Map<String, List<ResultRow>> byQuery = result.getRows().stream()
                .collect(Collectors.groupingBy(ResultRow::getQueryName));

        for (Map.Entry<String, List<ResultRow>> entry : byQuery.entrySet()) {
            System.out.println("Query Results: " + entry.getKey());
            System.out.println("-------------------------------------------------");
            
            if ("daily_traffic".equals(entry.getKey())) {
                System.out.println("LogDate\tStatusCode\tRequestCount\tTotalBytes");
                for (ResultRow r : entry.getValue()) {
                    System.out.println(r.getLogDate() + "\t" + r.getStatusCode() + "\t" + r.getRequestCount() + "\t" + r.getTotalBytes());
                }
            } else if ("top_resources".equals(entry.getKey())) {
                System.out.println("ResourcePath\tRequestCount\tTotalBytes\tDistinctHosts");
                for (ResultRow r : entry.getValue()) {
                    System.out.println(r.getResourcePath() + "\t" + r.getRequestCount() + "\t" + r.getTotalBytes() + "\t" + r.getDistinctHosts());
                }
            } else if ("hourly_errors".equals(entry.getKey())) {
                System.out.println("LogDate\tLogHour\tErrorRequests\tTotalRequests\tErrorRate\tDistinctErrorHosts");
                for (ResultRow r : entry.getValue()) {
                    System.out.println(r.getLogDate() + "\t" + r.getLogHour() + "\t" + r.getErrorRequestCount() + "\t" + 
                                       r.getTotalRequestCount() + "\t" + String.format("%.4f", r.getErrorRate()) + "\t" + r.getDistinctErrorHosts());
                }
            }
            System.out.println();
        }
    }
}
