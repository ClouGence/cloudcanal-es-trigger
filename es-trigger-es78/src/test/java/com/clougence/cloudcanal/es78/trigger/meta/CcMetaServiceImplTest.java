package com.clougence.cloudcanal.es78.trigger.meta;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.clougence.cloudcanal.es_base.EsConnConfig;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

/**
 * @author bucketli 2024/7/31 14:40:44
 */
public class CcMetaServiceImplTest {

    @Test
    public void testCreateTriggerIdx() {
        EsConnConfig connConfig = new EsConnConfig();
        connConfig.setHosts("120.27.247.43:9200");

        CcMetaService metaService = new CcMetaServiceImpl();
        metaService.createTriggerIdx(connConfig);
    }

    @Test
    public void testUpsertSettings() {
        EsConnConfig connConfig = new EsConnConfig();
        connConfig.setHosts("120.27.247.43:9200");

        CcMetaService metaService = new CcMetaServiceImpl();

        List<String> indexs = Arrays.asList(EsTriggerConstant.ES_TRIGGER_IDX);
        Map<String, Object> settings = new HashMap<>();
        settings.put(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY, 123L);
        metaService.upsertSettings(connConfig, indexs, settings);
    }
}
