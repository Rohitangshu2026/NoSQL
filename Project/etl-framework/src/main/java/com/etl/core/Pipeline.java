package com.etl.core;

public interface Pipeline {
    PipelineResult execute(ETLConfig config) throws Exception;
}
