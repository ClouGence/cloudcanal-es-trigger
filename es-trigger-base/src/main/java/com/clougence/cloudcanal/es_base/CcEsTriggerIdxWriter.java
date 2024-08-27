package com.clougence.cloudcanal.es_base;

import java.io.IOException;

/**
 * @author bucketli 2024/7/30 16:01:07
 */
public interface CcEsTriggerIdxWriter extends ComponentLifeCycle{

    void insertTriggerIdx(String idxName, TriggerEventType dataOp, String id, String docJson) throws IOException;
}
