package com.clougence.cloudcanal.es78.trigger.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.ClusterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es78.trigger.CcEsIdxTriggerPlugin;
import com.clougence.cloudcanal.es_base.EsConnConfig;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

import lombok.Getter;

/**
 * @author bucketli 2024/7/31 11:19:26
 */
public class Es7ClientConn {

    private static final Logger       log        = LoggerFactory.getLogger(Es7ClientConn.class);

    public static final Es7ClientConn instance   = new Es7ClientConn();

    public volatile AtomicBoolean     refreshing = new AtomicBoolean(false);

    @Getter
    private RestHighLevelClient       esClient;

    private Es7ClientConn(){
    }

    public void refreshByConfig(Map<String, Object> config) {
        String hosts = (String) (config.get(EsTriggerConstant.TRIGGER_IDX_HOST_KEY));

        if (StringUtils.isBlank(hosts)) {
            log.error(EsTriggerConstant.TRIGGER_IDX_HOST_KEY + " is blank,fail to create or re-create esClient.");
            return;
        }

        String user = (String) (config.get(EsTriggerConstant.TRIGGER_IDX_USER_KEY));
        String password = (String) (config.get(EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY));
        EsConnConfig connConfig = new EsConnConfig();
        connConfig.setHosts(hosts);
        connConfig.setUserName(user);
        connConfig.setPassword(password);

        reCreateEsClient(connConfig);
    }

    public void refreshBySettings(ClusterSettings settings) {
        Map<String, Object> config = new HashMap<>();

        String hosts = settings.get(CcEsIdxTriggerPlugin.triggerIdxHost);
        if (StringUtils.isBlank(hosts)) {
            log.error(EsTriggerConstant.TRIGGER_IDX_HOST_KEY + " is blank,fail to create or re-create esClient.");
            return;
        }

        config.put(EsTriggerConstant.TRIGGER_IDX_HOST_KEY, hosts);

        String user = settings.get(CcEsIdxTriggerPlugin.triggerIdxUser);
        config.put(EsTriggerConstant.TRIGGER_IDX_USER_KEY, user);

        String password = settings.get(CcEsIdxTriggerPlugin.triggerIdxPassword);
        config.put(EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY, password);

        refreshByConfig(config);
    }

    protected synchronized void reCreateEsClient(EsConnConfig connConfig) {
        try {
            refreshing.compareAndSet(false, true);

            try {
                if (esClient != null) {
                    esClient.close();
                    log.info("Es client close successfully before recreate.");
                }
            } catch (Exception e) {
                String msg = "Close es client failed,but ignore.msg:" + ExceptionUtils.getRootCauseMessage(e);
                log.error(msg);
            }

            try {
                esClient = Es7ClientHelper.generateEsClient(connConfig);
                log.info("Es client create successfully.");
            } catch (Exception e) {
                String msg = "Create es client failed,PLUGIN WILL NOT FUNCTION.msg:" + ExceptionUtils.getRootCauseMessage(e);
                log.error(msg);
            }
        } finally {
            refreshing.compareAndSet(true, false);
        }
    }
}
