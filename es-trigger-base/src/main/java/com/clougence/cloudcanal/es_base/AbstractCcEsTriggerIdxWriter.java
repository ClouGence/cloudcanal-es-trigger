package com.clougence.cloudcanal.es_base;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author bucketli 2024/8/27 10:12:03
 */
public abstract class AbstractCcEsTriggerIdxWriter implements Runnable, CcEsTriggerIdxWriter {

    private static final Logger            log                = LoggerFactory.getLogger(AbstractCcEsTriggerIdxWriter.class);

    private static final AtomicBoolean     inited             = new AtomicBoolean(false);

    private static final AtomicBoolean     triggerIdxIdInited = new AtomicBoolean(false);

    private final AtomicLong               incrementId        = new AtomicLong(0);

    private static final int               scnStep            = 100000;

    private long                           currentStepMaxVal  = 0;

    private ExecutorService                executor;

    protected static final int             cacheSize          = 65535;

    protected static final int             batchSize          = 1024;

    private static final DateTimeFormatter formatter          = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssSSS");

    @Override
    public void start() {
        if (inited.compareAndSet(false, true)) {
            log.info(this.getClass().getSimpleName() + " begin to start.");
            initTriggerIdxId();
            initWriterThread();
            log.info(this.getClass().getSimpleName() + " start successfully.");
        }
    }

    protected abstract String fetchScnCurrVal();

    protected abstract boolean isClientInited();

    protected abstract void updateIncreIdToNextStep(long nextStart);

    protected abstract void insertInner(Map<String, Object> doc, String srcIdx, String srcId);

    private void initTriggerIdxId() {
        try {
            if (!isClientInited()) {
                return;
            }

            String s = fetchScnCurrVal();
            long currVal;
            if (StringUtils.isBlank(s)) {
                currVal = 0;
            } else {
                currVal = Long.parseLong(s);
            }

            long nextStart = currVal + scnStep;
            updateIncreIdToNextStep(nextStart);

            currentStepMaxVal = nextStart;

            incrementId.set(currVal);

            triggerIdxIdInited.compareAndSet(false, true);
        } catch (Exception e) {
            String msg = "Init trigger index settings failed,init later.msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(msg, e);
            //            throw new RuntimeException(msg, e);
        }
    }

    private void initWriterThread() {
        executor = Executors.newFixedThreadPool(1);
        executor.execute(this);
    }

    @Override
    public void insertTriggerIdx(String idxName, TriggerEventType dataOp, String id, String docJson) throws IOException {
        if (isClientInited()) {
            log.warn("Es client is null,skip write data.");
            return;
        }

        Map<String, Object> doc = new HashMap<>();
        long gid = nextId();
        doc.put("scn", gid);
        doc.put("idx_name", idxName);
        doc.put("event_type", dataOp.getCode());
        doc.put("pk", id);

        if (docJson != null) {
            doc.put("row_data", docJson);
        }

        doc.put("create_time", LocalDateTime.now().format(formatter));

        insertInner(doc, idxName, id);
    }

    private synchronized long nextId() {
        if (!triggerIdxIdInited.get()) {
            initTriggerIdxId();

            if (!triggerIdxIdInited.get()) {
                throw new IllegalArgumentException("Trigger idx id can not be inited,maybe datasource not ready.");
            }
        }

        if (incrementId.get() >= currentStepMaxVal) {
            initTriggerIdxId();
        }

        return incrementId.incrementAndGet();
    }

    @Override
    public void stop() {
        if (inited.compareAndSet(true, false)) {
            try {
                log.info(this.getClass().getSimpleName() + " try to stop...");

                if (this.executor != null) {
                    this.executor.shutdown();
                }

                log.info(this.getClass().getSimpleName() + " stop successfully.");
            } catch (Exception e) {
                log.warn(this.getClass().getSimpleName() + " stop failed,but ignore.msg:" + ExceptionUtils.getRootCauseMessage(e));
            }
        }
    }
}
