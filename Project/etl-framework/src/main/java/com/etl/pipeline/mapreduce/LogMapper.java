package com.etl.pipeline.mapreduce;

import com.etl.core.BatchSplitter;
import com.etl.core.LogParser;
import com.etl.core.LogRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Optional;

public class LogMapper extends Mapper<LongWritable, Text, Text, Text> {
    public enum Counters {
        TOTAL_RECORDS,
        MALFORMED_RECORDS
    }

    private final Text outKey   = new Text();
    private final Text outValue = new Text();
    private String queryName;
    private int batchId;

    @Override
    protected void setup(Context context) {
        queryName = context.getConfiguration().get("etl.query.name");

        // Each batch file becomes one or more input splits. The file name carries
        // the batch id (batch-NNNNN.log), which is the only reliable way for the
        // mapper to know which batch a record belongs to.
        InputSplit split = context.getInputSplit();
        if (split instanceof FileSplit) {
            batchId = BatchSplitter.batchIdFromFilename(
                    ((FileSplit) split).getPath().getName());
        } else {
            batchId = 0;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        context.getCounter(Counters.TOTAL_RECORDS).increment(1);

        Optional<LogRecord> optRecord = LogParser.parse(line);
        if (!optRecord.isPresent()) {
            context.getCounter(Counters.MALFORMED_RECORDS).increment(1);
            return;
        }

        LogRecord record = optRecord.get();

        if ("daily_traffic".equals(queryName)) {
            outKey.set(batchId + "|" + record.getLogDate() + "|" + record.getStatusCode());
            outValue.set(String.valueOf(record.getBytes()));
            context.write(outKey, outValue);
        } else if ("top_resources".equals(queryName)) {
            outKey.set(batchId + "|" + record.getResourcePath());
            outValue.set(record.getHost() + "|" + record.getBytes());
            context.write(outKey, outValue);
        } else if ("hourly_errors".equals(queryName)) {
            outKey.set(batchId + "|" + record.getLogDate() + "|" + record.getLogHour());
            outValue.set(record.getStatusCode() + "|" + record.getHost());
            context.write(outKey, outValue);
        }
    }
}
