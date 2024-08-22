package com.clougence.cloudcanal.es8.trigger.writer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import co.elastic.clients.elasticsearch.core.UpdateRequest;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es8.trigger.ds.Es8ClientConn;
import com.clougence.cloudcanal.es_base.CcEsTriggerIdxWriter;
import com.clougence.cloudcanal.es_base.ComponentLifeCycle;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;
import com.clougence.cloudcanal.es_base.TriggerEventType;

import co.elastic.clients.elasticsearch.indices.GetIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.GetIndicesSettingsResponse;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.PutIndicesSettingsResponse;
import co.elastic.clients.json.JsonData;

/**
 * @author bucketli 2024/7/30 16:06:32
 */
public class CcEs8TriggerIdxWriterImpl implements CcEsTriggerIdxWriter, ComponentLifeCycle {

    private static final AtomicBoolean inited             = new AtomicBoolean(false);

    private static final AtomicBoolean triggerIdxIdInited = new AtomicBoolean(false);

    private static final Logger        log                = LoggerFactory.getLogger(CcEs8TriggerIdxWriterImpl.class);

    private final AtomicLong           incrementId        = new AtomicLong(0);

    private long                       currentStepMaxVal  = 0;

    @Override
    public void start() {
        if (inited.compareAndSet(false, true)) {
            log.info(this.getClass().getSimpleName() + " begin to start.");
            initTriggerIdxId();
            log.info(this.getClass().getSimpleName() + " start successfully.");
        }
    }

    private void initTriggerIdxId() {
        try {
            if (Es8ClientConn.instance.getEsClient() == null) {
                return;
            }

            GetIndicesSettingsRequest req = new GetIndicesSettingsRequest.Builder().index(EsTriggerConstant.ES_TRIGGER_IDX).build();
            GetIndicesSettingsResponse res = Es8ClientConn.instance.getEsClient().indices().getSettings(req);
            Object s = res.result().get(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY);
            if (s == null) {
                updateIncreIdToNextStep(0);
            } else {
                updateIncreIdToNextStep(Long.parseLong(String.valueOf(s)));
            }

            triggerIdxIdInited.compareAndSet(false, true);
        } catch (Exception e) {
            String msg = "Init trigger index settings failed,init later.msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(msg, e);
            //            throw new RuntimeException(msg, e);
        }
    }

    private void updateIncreIdToNextStep(long currentId) {
        try {
            long nextVal = currentId + 10000;
            PutIndicesSettingsRequest req = new PutIndicesSettingsRequest.Builder().index(EsTriggerConstant.ES_TRIGGER_IDX)
                .settings(t -> t.otherSettings(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY, JsonData.of(nextVal)))
                .build();
            PutIndicesSettingsResponse res = Es8ClientConn.instance.getEsClient().indices().putSettings(req);
            if (!res.acknowledged()) {
                throw new RuntimeException("Update trigger index settings failed, acknowledged is false.");
            }

            log.info("Updated incremental id,currStepMaxVal:" + nextVal);

            currentStepMaxVal = nextVal;
        } catch (Exception e) {
            String msg = "Update trigger index settings failed.msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    private synchronized long nextId() {
        if (!triggerIdxIdInited.get()) {
            initTriggerIdxId();

            if (!triggerIdxIdInited.get()) {
                throw new IllegalArgumentException("Trigger idx id can not be inited,maybe datasource not ready.");
            }
        }

        if (incrementId.get() > currentStepMaxVal) {
            updateIncreIdToNextStep(incrementId.get());
            return nextId();
        } else {
            return incrementId.incrementAndGet();
        }
    }

    @Override
    public void stop() {
        if (inited.compareAndSet(true, false)) {
            log.info(this.getClass().getSimpleName() + " stop successfully.");
        }
    }

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssSSS");

    @Override
    public void insertTriggerIdx(String idxName, TriggerEventType dataOp, String id, String docJson) throws IOException {
        if (Es8ClientConn.instance.getEsClient() == null) {
            log.warn("Es client is null,skip write data.");
            return;
        }

        Map<String, Object> re = new HashMap<>();
        long gid = nextId();
        re.put("scn", gid);
        re.put("idx_name", idxName);
        re.put("event_type", dataOp.getCode());
        re.put("pk", id);

        if (docJson != null) {
            re.put("row_data", docJson);
        }

        re.put("create_time", LocalDateTime.now().format(formatter));

        UpdateRequest<String, Object> ur = new UpdateRequest.Builder<String, Object>().index(EsTriggerConstant.ES_TRIGGER_IDX).id(gid + "").doc(re).docAsUpsert(true).build();
        Es8ClientConn.instance.getEsClient().update(ur, String.class);
    }
}
