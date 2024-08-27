package com.clougence.cloudcanal.es8.trigger.meta;

import static com.clougence.cloudcanal.es_base.EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.junit.jupiter.api.Test;

import com.clougence.cloudcanal.es8.trigger.ds.Es8ClientHelper;
import com.clougence.cloudcanal.es_base.CcMetaService;
import com.clougence.cloudcanal.es_base.EsConnConfig;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

import co.elastic.clients.elasticsearch.core.UpdateRequest;

/**
 * @author bucketli 2024/7/31 14:40:44
 */
public class CcEs8MetaServiceImplTest {

    @Test
    public void testCreateTriggerIdx() {
        EsConnConfig connConfig = new EsConnConfig();
        connConfig.setHosts("120.27.128.105:9200");

        CcMetaService metaService = new CcEs8MetaServiceImpl();
        metaService.createTriggerIdx(connConfig);
    }

    @Test
    public void testUpsertSettings() {
        EsConnConfig connConfig = new EsConnConfig();
        connConfig.setHosts("120.27.128.105:9200");

        CcMetaService metaService = new CcEs8MetaServiceImpl();

        List<String> indexs = Collections.singletonList(EsTriggerConstant.ES_TRIGGER_IDX);
        Map<String, Object> settings = new HashMap<>();
        settings.put(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY, 123L);
        metaService.upsertIndexSettings(connConfig, indexs, settings);
    }

    @Test
    public void testUpsertSettings_2() {
        EsConnConfig connConfig = new EsConnConfig();
        connConfig.setHosts("120.27.128.105:9200");

        CcMetaService metaService = new CcEs8MetaServiceImpl();

        List<String> indexs = Collections.singletonList("cc_my-vgpq6q097174t6t_user_db_product");
        Map<String, Object> settings = new HashMap<>();
        settings.put(EsTriggerConstant.IDX_ENABLE_CDC_CONF_KEY, true);
        metaService.upsertIndexSettings(connConfig, indexs, settings);
    }

    @Test
    public void testUpsertNodeSettings() {
        EsConnConfig connConfig = new EsConnConfig();
        connConfig.setHosts("101.37.24.239:9200");

        Map<String, Object> re = new HashMap<>();
        re.put(EsTriggerConstant.TRIGGER_IDX_HOST_KEY, "127.0.0.1:9200");
        re.put(EsTriggerConstant.TRIGGER_IDX_USER_KEY, null);
        re.put(EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY, null);

        CcMetaService metaService = new CcEs8MetaServiceImpl();
        metaService.upsertNodeSettings(connConfig, re);
    }

    @Test
    public void testQueryCcNodeSettings() {
        EsConnConfig connConfig = new EsConnConfig();
        connConfig.setHosts("101.37.24.239:9200");

        CcMetaService metaService = new CcEs8MetaServiceImpl();
        Map<String, Object> re = metaService.queryCcNodeSettings(connConfig);
        for (Map.Entry<String, Object> entry : re.entrySet()) {
            if (entry.getValue() == null) {
                System.out.println(entry.getKey() + " is null");
            } else {
                System.out.println(entry.getKey() + "  " + entry.getValue());
            }
        }
    }

    @Test
    public void testQueryIndexSettings() {
        EsConnConfig connConfig = new EsConnConfig();
        connConfig.setHosts("101.37.24.239:9200");

        CcMetaService metaService = new CcEs8MetaServiceImpl();
        Map<String, Object> re = metaService.queryIndexCcSettings(connConfig, EsTriggerConstant.ES_TRIGGER_IDX, Arrays.asList(TRIGGER_IDX_MAX_SCN_KEY));
        for (Map.Entry<String, Object> entry : re.entrySet()) {
            if (entry.getValue() == null) {
                System.out.println(entry.getKey() + " is null");
            } else {
                System.out.println(entry.getKey() + "  " + entry.getValue());
            }
        }
    }
}
