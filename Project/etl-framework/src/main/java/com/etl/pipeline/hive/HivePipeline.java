package com.etl.pipeline.hive;

import com.etl.core.ETLConfig;
import com.etl.core.Pipeline;
import com.etl.core.PipelineResult;

public class HivePipeline implements Pipeline {
    @Override
    public PipelineResult execute(ETLConfig config) throws Exception {
        throw new UnsupportedOperationException("Hive pipeline not yet implemented");
    }
}
