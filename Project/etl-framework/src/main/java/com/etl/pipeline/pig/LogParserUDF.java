package com.etl.pipeline.pig;

import com.etl.core.LogParser;
import com.etl.core.LogRecord;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.util.Optional;

public class LogParserUDF extends EvalFunc<Tuple> {
    private TupleFactory tupleFactory = TupleFactory.getInstance();

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        try {
            String line = (String) input.get(0);
            Optional<LogRecord> optRecord = LogParser.parse(line);

            if (!optRecord.isPresent()) {
                return null;
            }

            LogRecord record = optRecord.get();
            Tuple tuple = tupleFactory.newTuple(8);
            tuple.set(0, record.getHost());
            tuple.set(1, record.getLogDate());
            tuple.set(2, record.getLogHour());
            tuple.set(3, record.getMethod());
            tuple.set(4, record.getResourcePath());
            tuple.set(5, record.getProtocol());
            tuple.set(6, record.getStatusCode());
            tuple.set(7, record.getBytes());

            return tuple;
        } catch (Exception e) {
            return null;
        }
    }
}
