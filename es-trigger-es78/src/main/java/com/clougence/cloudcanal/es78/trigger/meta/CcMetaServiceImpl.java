package com.clougence.cloudcanal.es78.trigger.meta;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es78.trigger.ds.Es7ClientHelper;
import com.clougence.cloudcanal.es_base.EsConnConfig;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

/**
 * @author bucketli 2024/7/30 14:40:10
 */
public class CcMetaServiceImpl implements CcMetaService {

    private static final Logger log = LoggerFactory.getLogger(CcMetaServiceImpl.class);

    @Override
    public void createTriggerIdx(EsConnConfig connConfig) {
        try {
            Map<String, Object> properties = new HashMap<>();
            Map<String, Object> scnF = new HashMap<>();
            scnF.put("type", "long");
            scnF.put("index", true);
            properties.put("scn", scnF);

            Map<String, Object> idxNameF = new HashMap<>();
            idxNameF.put("type", "text");
            idxNameF.put("index", true);
            idxNameF.put("analyzer", "standard");
            properties.put("idx_name", idxNameF);

            //1:INSERT,2:UPDATE,3:DELETE
            Map<String, Object> eventTypeF = new HashMap<>();
            eventTypeF.put("type", "short");
            eventTypeF.put("index", true);
            properties.put("event_type", eventTypeF);

            Map<String, Object> rowDataF = new HashMap<>();
            rowDataF.put("type", "nested");
            properties.put("row_data", rowDataF);

            Map<String, Object> cDateF = new HashMap<>();
            cDateF.put("type", "date");
            cDateF.put("index", true);
            cDateF.put("format", "yyyy-MM-dd'T'HH:mm:ssSSS");
            properties.put("create_time", cDateF);

            Map<String, Object> mapping = new HashMap<>();
            mapping.put("properties", properties);

            createIdx(EsTriggerConstant.ES_TRIGGER_IDX, mapping, connConfig);
        } catch (Exception e) {
            String errMsg = "Create index " + EsTriggerConstant.ES_TRIGGER_IDX + " failed,msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void upsertSettings(EsConnConfig connConfig, List<String> indexes, Map<String, Object> settings) {
        try {
            Settings.Builder sb = Settings.builder();
            Long maxScn = (Long) (settings.get(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY));
            if (maxScn != null) {
                sb.put(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY, maxScn);
            }

            String hosts = (String) (settings.get(EsTriggerConstant.TRIGGER_IDX_HOST_KEY));
            if (StringUtils.isNotBlank(hosts)) {
                sb.put(EsTriggerConstant.TRIGGER_IDX_HOST_KEY, hosts);
            }

            String user = (String) (settings.get(EsTriggerConstant.TRIGGER_IDX_USER_KEY));
            if (StringUtils.isNotBlank(user)) {
                sb.put(EsTriggerConstant.TRIGGER_IDX_USER_KEY, user);
            }

            String password = (String) (settings.get(EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY));
            if (StringUtils.isNotBlank(password)) {
                sb.put(EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY, password);
            }

            UpdateSettingsRequest req = new UpdateSettingsRequest().indices(indexes.toArray(new String[] {})).settings(sb);

            RestHighLevelClient esClient = Es7ClientHelper.generateEsClient(connConfig);
            AcknowledgedResponse res = esClient.indices().putSettings(req, RequestOptions.DEFAULT);
            if (!res.isAcknowledged()) {
                throw new RuntimeException("Update trigger index settings failed, acknowledged is false.");
            }
        } catch (Exception e) {
            String errMsg = "Upsert settings to indexes failed,msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    protected void createIdx(String indexName, Map<String, Object> mapping, EsConnConfig connConfig) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.mapping(mapping);

        RestHighLevelClient esClient = Es7ClientHelper.generateEsClient(connConfig);
        CreateIndexResponse response = esClient.indices().create(request, RequestOptions.DEFAULT);
        if (!response.isAcknowledged()) {
            throw new RuntimeException("Index " + indexName + " create failed, acknowledged is false.");
        }
    }
}
