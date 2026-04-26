package com.etl.pipeline.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class QueryReducer extends Reducer<Text, Text, Text, Text> {
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String queryName = context.getConfiguration().get("etl.query.name");

        if ("daily_traffic".equals(queryName)) {
            long requestCount = 0;
            long totalBytes = 0;
            for (Text val : values) {
                requestCount++;
                totalBytes += Long.parseLong(val.toString());
            }
            outValue.set(requestCount + "\t" + totalBytes);
            context.write(key, outValue);

        } else if ("top_resources".equals(queryName)) {
            long requestCount = 0;
            long totalBytes = 0;
            Set<String> hosts = new HashSet<>();
            for (Text val : values) {
                String[] parts = val.toString().split("\\|");
                hosts.add(parts[0]);
                totalBytes += Long.parseLong(parts[1]);
                requestCount++;
            }
            outValue.set(requestCount + "\t" + totalBytes + "\t" + hosts.size());
            context.write(key, outValue);

        } else if ("hourly_errors".equals(queryName)) {
            long errorRequestCount = 0;
            long totalRequestCount = 0;
            Set<String> errorHosts = new HashSet<>();

            for (Text val : values) {
                String[] parts = val.toString().split("\\|");
                int statusCode = Integer.parseInt(parts[0]);
                String host = parts[1];

                totalRequestCount++;
                if (statusCode >= 400 && statusCode <= 599) {
                    errorRequestCount++;
                    errorHosts.add(host);
                }
            }

            double errorRate = totalRequestCount == 0 ? 0.0 : (double) errorRequestCount / totalRequestCount;
            outValue.set(errorRequestCount + "\t" + totalRequestCount + "\t" + errorRate + "\t" + errorHosts.size());
            context.write(key, outValue);
        }
    }
}
