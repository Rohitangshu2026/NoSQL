package com.etl.pipeline.mongodb;

import com.etl.core.ETLConfig;
import com.etl.core.Pipeline;
import com.etl.core.PipelineResult;

public class MongoDBPipeline implements Pipeline {
    @Override
    public PipelineResult execute(ETLConfig config) throws Exception {
        throw new UnsupportedOperationException("MongoDB pipeline not yet implemented");
    }
}
