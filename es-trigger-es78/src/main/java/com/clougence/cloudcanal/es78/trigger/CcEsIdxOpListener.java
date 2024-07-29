package com.clougence.cloudcanal.es78.trigger;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es_base.ComponentLifeCycle;

/**
 * @author bucketli 2024/6/26 17:48:55
 */
public class CcEsIdxOpListener implements IndexingOperationListener, ComponentLifeCycle, Consumer<Boolean> {

    private static final Logger        log                     = LoggerFactory.getLogger(CcEsIdxOpListener.class);

    private static final AtomicBoolean inited                  = new AtomicBoolean(false);

    public static final String         IDX_ENABLE_CDC_CONF_KEY = "index.cdc.enabled";

    private final IndexModule          indexModule;

    public CcEsIdxOpListener(IndexModule indexModule){
        this.indexModule = indexModule;
    }

    @Override
    public void accept(Boolean cdcEnabled) {
    }

    @Override
    public void start() {
        if (inited.compareAndSet(false, true)) {
            log.info("Component " + this.getClass().getSimpleName() + " start successfully.");
        }
    }

    @Override
    public void stop() {
        if (inited.compareAndSet(true, false)) {
            log.info("Component " + this.getClass().getSimpleName() + " stop successfully.");
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        log.info("receive DELETE event.");
        if (delete.origin() != Engine.Operation.Origin.PRIMARY // not primary shard
            || !indexModule.getSettings().getAsBoolean(IDX_ENABLE_CDC_CONF_KEY, false) // not enable cdc
            || result.getFailure() != null // failed operation
            || !result.isFound()) { // not found
            return;
        }

        String indexName = shardId.getIndex().getName();
        String delId = delete.id();

        log.info("[DELETE] " + indexName + " data,pk:" + delId);
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        log.info("receive INDEX event.");
        if (index.origin() != Engine.Operation.Origin.PRIMARY // not primary shard
            || !indexModule.getSettings().getAsBoolean(IDX_ENABLE_CDC_CONF_KEY, false) // cdc not enabled
            || result.getFailure() != null // has failure
            || result.getResultType() != Engine.Result.Type.SUCCESS) { // not success
            return;
        }

        String indexName = shardId.getIndex().getName();
        ParsedDocument doc = index.parsedDoc();

        if (result.isCreated()) {
            log.info("[INSERT] " + indexName + " data,data:" + doc.source().utf8ToString());
        } else {
            log.info("[UPDATE] " + indexName + " data,data:" + doc.source().utf8ToString());
        }
    }
}
