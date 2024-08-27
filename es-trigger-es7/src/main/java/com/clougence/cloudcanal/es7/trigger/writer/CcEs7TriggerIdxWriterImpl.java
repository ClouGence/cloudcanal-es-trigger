package com.clougence.cloudcanal.es7.trigger.writer;

import static com.clougence.cloudcanal.es_base.EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es7.trigger.ds.Es7ClientConn;
import com.clougence.cloudcanal.es_base.AbstractCcEsTriggerIdxWriter;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

/**
 * @author bucketli 2024/7/30 16:06:32
 */
public class CcEs7TriggerIdxWriterImpl extends AbstractCcEsTriggerIdxWriter {

    private static final Logger           log   = LoggerFactory.getLogger(CcEs7TriggerIdxWriterImpl.class);

    protected BlockingQueue<IndexRequest> cache = new ArrayBlockingQueue<>(cacheSize);

    protected boolean isClientInited() { return Es7ClientConn.instance.getEsClient() != null; }

    protected String fetchScnCurrVal() {
        try {
            GetSettingsRequest req = new GetSettingsRequest().indices(EsTriggerConstant.ES_TRIGGER_IDX);
            GetSettingsResponse res = Es7ClientConn.instance.getEsClient().indices().getSettings(req, RequestOptions.DEFAULT);
            return res.getSetting(EsTriggerConstant.ES_TRIGGER_IDX, TRIGGER_IDX_MAX_SCN_KEY);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void updateIncreIdToNextStep(long nextStart) {
        try {
            Settings.Builder sb = Settings.builder().put(TRIGGER_IDX_MAX_SCN_KEY, nextStart);
            UpdateSettingsRequest req = new UpdateSettingsRequest().indices(EsTriggerConstant.ES_TRIGGER_IDX).settings(sb);

            AcknowledgedResponse res = Es7ClientConn.instance.getEsClient().indices().putSettings(req, RequestOptions.DEFAULT);
            if (!res.isAcknowledged()) {
                throw new RuntimeException("Update trigger index settings failed, acknowledged is false.");
            }

            log.info("Updated " + TRIGGER_IDX_MAX_SCN_KEY + " to " + nextStart);
        } catch (Exception e) {
            String msg = "Update trigger index settings failed.msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    @Override
    protected void insertInner(Map<String, Object> doc, String srcIdx, String srcId) {
        IndexRequest ir = new IndexRequest().index(EsTriggerConstant.ES_TRIGGER_IDX).source(doc);

        try {
            boolean offered = this.cache.offer(ir, 2, TimeUnit.SECONDS);
            if (!offered) {
                log.warn("Offer to write cache timeout cause no space left,just skip and record here,idx_name:" + srcIdx + ",_id:" + srcId);
            } else {
                log.info("Offer document to cache success,_id:" + srcId);
            }
        } catch (InterruptedException e) {
            log.warn("Offer to cache interruppted,but skip,idx_name:" + srcIdx + ",_id:" + srcId);
        }
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                List<IndexRequest> irs = new ArrayList<>();
                int real = cache.drainTo(irs, batchSize);
                log.info("Drain " + real + " documents from cache");
                if (real > 0) {
                    BulkRequest reqs = new BulkRequest();
                    for (IndexRequest ir : irs) {
                        reqs.add(ir);
                    }

                    WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.NONE;
                    reqs.setRefreshPolicy(refreshPolicy);

                    log.info("Try to bulk documents,real:" + real);

                    BulkResponse bulkResponse = Es7ClientConn.instance.getEsClient().bulk(reqs, RequestOptions.DEFAULT);

                    for (BulkItemResponse response : bulkResponse) {
                        if (response.isFailed()) {
                            String errMsg = "bulk put FAILED!msg:" + response.getFailureMessage() + ",action id: " + response.getId();
                            log.error(errMsg);
                            throw new RuntimeException(errMsg);
                        }
                    }

                    log.info("Bulk documents success,real:" + real);
                }

                //no need to sleep
                if (real < batchSize) {
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            log.error("Consume request from queue failed.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }
}
