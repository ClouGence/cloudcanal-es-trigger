package com.clougence.cloudcanal.es78.trigger.writer;

import java.io.IOException;

import org.elasticsearch.index.mapper.ParsedDocument;

import com.clougence.cloudcanal.es78.trigger.DataOp;

/**
 * @author bucketli 2024/7/30 16:01:07
 */
public interface CcEsTriggerIdxWriter {

    void insertTriggerIdx(String idxName, DataOp dataOp, String id, ParsedDocument doc) throws IOException;
}
