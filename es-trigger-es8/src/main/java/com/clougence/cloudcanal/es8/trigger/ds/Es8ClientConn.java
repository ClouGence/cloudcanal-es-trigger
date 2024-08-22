package com.clougence.cloudcanal.es8.trigger.ds;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.common.settings.ClusterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es8.trigger.CcEs8IdxTriggerPlugin;
import com.clougence.cloudcanal.es_base.EsConnConfig;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import lombok.Getter;

/**
 * @author bucketli 2024/7/31 11:19:26
 */
public class Es8ClientConn {

    private static final Logger              log         = LoggerFactory.getLogger(Es8ClientConn.class);

    public static final Es8ClientConn        instance    = new Es8ClientConn();

    public volatile AtomicBoolean            refreshing  = new AtomicBoolean(false);

    @Getter
    private ElasticsearchClient              esClient;

    private static final Map<String, Object> configInMem = new ConcurrentHashMap<>();

    private Es8ClientConn(){
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

    public void addHostSettingConsumer(ClusterSettings settings) {
        String hosts = settings.get(CcEs8IdxTriggerPlugin.triggerIdxHost);
        configInMem.put(EsTriggerConstant.TRIGGER_IDX_HOST_KEY, hosts);

        String user = settings.get(CcEs8IdxTriggerPlugin.triggerIdxUser);
        configInMem.put(EsTriggerConstant.TRIGGER_IDX_USER_KEY, user);

        String password = settings.get(CcEs8IdxTriggerPlugin.triggerIdxPassword);
        configInMem.put(EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY, password);

        log.info("Init host settings,hosts:" + hosts + ",user:" + user);

        settings.addSettingsUpdateConsumer(CcEs8IdxTriggerPlugin.triggerIdxHost, s -> configChange(EsTriggerConstant.TRIGGER_IDX_HOST_KEY, s));
        settings.addSettingsUpdateConsumer(CcEs8IdxTriggerPlugin.triggerIdxUser, s -> configChange(EsTriggerConstant.TRIGGER_IDX_USER_KEY, s));
        settings.addSettingsUpdateConsumer(CcEs8IdxTriggerPlugin.triggerIdxPassword, s -> configChange(EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY, s));

        refreshByConfig(configInMem);
    }

    protected void configChange(String propertyName, Object propertyValue) {
        configInMem.put(propertyName, propertyValue);
        refreshByConfig(configInMem);
    }

    protected synchronized void reCreateEsClient(EsConnConfig connConfig) {
        try {
            refreshing.compareAndSet(false, true);

            try {
                esClient = Es8ClientHelper.generateEsClient(connConfig);
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
