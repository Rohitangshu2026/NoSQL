package com.etl;

import com.etl.core.Pipeline;
import com.etl.pipeline.mapreduce.MapReducePipeline;
import com.etl.pipeline.pig.PigPipeline;
import com.etl.pipeline.mongodb.MongoDBPipeline;
import com.etl.pipeline.hive.HivePipeline;

public class PipelineFactory {
    public static Pipeline create(String name) {
        switch (name.toLowerCase()) {
            case "mapreduce":
                return new MapReducePipeline();
            case "pig":
                return new PigPipeline();
            case "mongodb":
                return new MongoDBPipeline();
            case "hive":
                return new HivePipeline();
            default:
                throw new IllegalArgumentException("Unknown pipeline: " + name);
        }
    }
}
