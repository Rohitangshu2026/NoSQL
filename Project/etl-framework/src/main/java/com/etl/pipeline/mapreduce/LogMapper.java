package com.etl.pipeline.mapreduce;

import com.etl.core.LogParser;
import com.etl.core.LogRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

public class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
    public enum Counters {
        TOTAL_RECORDS,
        MALFORMED_RECORDS
    }

    private Text outKey = new Text();
    private Text outValue = new Text();
    private String queryName;

    @Override
    protected void setup(Context context) {
        queryName = context.getConfiguration().get("etl.query.name");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Optional<LogRecord> optRecord = LogParser.parse(line);
        context.getCounter(Counters.TOTAL_RECORDS).increment(1);

        if (!optRecord.isPresent()) {
            context.getCounter(Counters.MALFORMED_RECORDS).increment(1);
            return;
        }

        LogRecord record = optRecord.get();
//        int batchId = context.getConfiguration().getInt("mapreduce.task.partition", 0);

        if ("daily_traffic".equals(queryName)) {
            outKey.set(record.getLogDate() + "|" + record.getStatusCode());
            outValue.set(String.valueOf(record.getBytes()));
            context.write(outKey, outValue);
        } else if ("top_resources".equals(queryName)) {
            outKey.set(record.getResourcePath());
            outValue.set(record.getHost() + "|" + record.getBytes());
            context.write(outKey, outValue);
        } else if ("hourly_errors".equals(queryName)) {
            outKey.set(record.getLogDate() + "|" + record.getLogHour());
            outValue.set(record.getStatusCode() + "|" + record.getHost());
            context.write(outKey, outValue);
        }
    }
}
