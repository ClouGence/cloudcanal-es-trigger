package com.clougence.cloudcanal.es78.trigger.writer;

import org.elasticsearch.index.mapper.ParsedDocument;

import com.clougence.cloudcanal.es78.trigger.DataOp;

import java.io.IOException;

/**
 * @author bucketli 2024/7/30 16:01:07
 */
public interface CcEsTriggerIdxWriter {

    void insertTriggerIdx(String idxName, DataOp dataOp, String id, ParsedDocument doc, long genTs, long seqNo) throws IOException;
}
