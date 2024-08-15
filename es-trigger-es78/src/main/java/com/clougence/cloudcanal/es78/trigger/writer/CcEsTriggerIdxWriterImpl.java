package com.clougence.cloudcanal.es78.trigger.writer;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es78.trigger.TriggerEventType;
import com.clougence.cloudcanal.es78.trigger.ds.Es7ClientConn;
import com.clougence.cloudcanal.es_base.ComponentLifeCycle;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

/**
 * @author bucketli 2024/7/30 16:06:32
 */
public class CcEsTriggerIdxWriterImpl implements CcEsTriggerIdxWriter, ComponentLifeCycle {

    private static final AtomicBoolean inited             = new AtomicBoolean(false);

    private static final AtomicBoolean triggerIdxIdInited = new AtomicBoolean(false);

    private static final Logger        log                = LoggerFactory.getLogger(CcEsTriggerIdxWriterImpl.class);

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
            if (Es7ClientConn.instance.getEsClient() == null) {
                return;
            }

            GetSettingsRequest req = new GetSettingsRequest().indices(EsTriggerConstant.ES_TRIGGER_IDX);
            GetSettingsResponse res = Es7ClientConn.instance.getEsClient().indices().getSettings(req, RequestOptions.DEFAULT);
            String s = res.getSetting(EsTriggerConstant.ES_TRIGGER_IDX, EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY);
            if (StringUtils.isBlank(s)) {
                updateIncreIdToNextStep(0);
            } else {
                updateIncreIdToNextStep(Long.parseLong(s));
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
            Settings.Builder sb = Settings.builder().put(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY, nextVal);
            UpdateSettingsRequest req = new UpdateSettingsRequest().indices(EsTriggerConstant.ES_TRIGGER_IDX).settings(sb);

            AcknowledgedResponse res = Es7ClientConn.instance.getEsClient().indices().putSettings(req, RequestOptions.DEFAULT);
            if (!res.isAcknowledged()) {
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
    public void insertTriggerIdx(String idxName, TriggerEventType dataOp, String id, ParsedDocument doc) throws IOException {
        if (Es7ClientConn.instance.getEsClient() == null) {
            log.warn("Es client is null,skip write data.");
            return;
        }

        Map<String, Object> re = new HashMap<>();
        long gid = nextId();
        re.put("scn", gid);
        re.put("idx_name", idxName);
        re.put("event_type", dataOp.getCode());
        re.put("pk", id);

        if (doc != null && doc.source() != null) {
            re.put("row_data", doc.source().utf8ToString());
        }

        re.put("create_time", LocalDateTime.now().format(formatter));

        UpdateRequest ur = new UpdateRequest().index(EsTriggerConstant.ES_TRIGGER_IDX).id(gid + "").doc(re).docAsUpsert(true);
        Es7ClientConn.instance.getEsClient().update(ur, RequestOptions.DEFAULT);
    }
}
