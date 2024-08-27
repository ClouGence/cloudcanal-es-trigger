package com.clougence.cloudcanal.es8.trigger.meta;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es8.trigger.ds.Es8ClientHelper;
import com.clougence.cloudcanal.es_base.CcMetaService;
import com.clougence.cloudcanal.es_base.EsConnConfig;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.mapping.*;
import co.elastic.clients.elasticsearch.cluster.GetClusterSettingsRequest;
import co.elastic.clients.elasticsearch.cluster.GetClusterSettingsResponse;
import co.elastic.clients.elasticsearch.cluster.PutClusterSettingsRequest;
import co.elastic.clients.elasticsearch.cluster.PutClusterSettingsResponse;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.json.JsonData;

/** @author bucketli 2024/7/30 14:40:10 */
public class CcEs8MetaServiceImpl implements CcMetaService {

    private static final Logger log = LoggerFactory.getLogger(CcEs8MetaServiceImpl.class);

    @Override
    public void createTriggerIdx(EsConnConfig connConfig) {
        try {
            Map<String, Property> properties = new HashMap<>();

            Property.Builder scnF = new Property.Builder();
            LongNumberProperty scnP = new LongNumberProperty.Builder().build();
            scnF.long_(scnP);
            properties.put("scn", scnF.build());

            Property.Builder idxNameF = new Property.Builder();
            TextProperty.Builder idxNameP = new TextProperty.Builder();
            idxNameP.index(true);
            idxNameP.analyzer("standard");
            idxNameF.text(idxNameP.build());
            properties.put("idx_name", idxNameF.build());

            //I:INSERT,U:UPDATE,D:DELETE
            Property.Builder eventTypeF = new Property.Builder();
            TextProperty.Builder eventTypeP = new TextProperty.Builder();
            eventTypeP.index(true);
            eventTypeP.analyzer("standard");
            eventTypeF.text(eventTypeP.build());
            properties.put("event_type", eventTypeF.build());

            Property.Builder pkF = new Property.Builder();
            TextProperty.Builder pkP = new TextProperty.Builder();
            pkP.index(true);
            pkP.analyzer("standard");
            pkF.text(pkP.build());
            properties.put("pk", pkF.build());

            Property.Builder rowDataF = new Property.Builder();
            TextProperty.Builder rowDataP = new TextProperty.Builder();
            rowDataP.index(true);
            rowDataP.analyzer("standard");
            rowDataF.text(rowDataP.build());
            properties.put("row_data", rowDataF.build());

            Property.Builder cDateF = new Property.Builder();
            DateProperty.Builder cDateP = new DateProperty.Builder();
            cDateP.format("yyyy-MM-dd'T'HH:mm:ssSSS");
            cDateP.index(true);
            cDateF.date(cDateP.build());
            properties.put("create_time", cDateF.build());

            TypeMapping typeMapping = new TypeMapping.Builder().properties(properties).build();

            createIdx(EsTriggerConstant.ES_TRIGGER_IDX, typeMapping, connConfig);
        } catch (Exception e) {
            String errMsg = "Create index " + EsTriggerConstant.ES_TRIGGER_IDX + " failed,msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void upsertIndexSettings(EsConnConfig connConfig, List<String> indexes, Map<String, Object> settings) {
        try {
            Map<String, JsonData> settingMap = new HashMap<>();
            Long maxScn = (Long) (settings.get(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY));
            if (maxScn != null) {
                settingMap.put(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY, JsonData.of(maxScn));
            }

            Object enable = settings.get(EsTriggerConstant.IDX_ENABLE_CDC_CONF_KEY);
            if (enable != null) {
                settingMap.put(EsTriggerConstant.IDX_ENABLE_CDC_CONF_KEY, JsonData.of((Boolean) enable));
            }

            PutIndicesSettingsRequest req = new PutIndicesSettingsRequest.Builder().index(indexes).settings(t -> t.otherSettings(settingMap)).build();

            ElasticsearchClient esClient = Es8ClientHelper.generateEsClient(connConfig);
            PutIndicesSettingsResponse res = esClient.indices().putSettings(req);
            if (!res.acknowledged()) {
                throw new RuntimeException("Update trigger index settings failed, acknowledged is false.");
            }
        } catch (Exception e) {
            String errMsg = "Upsert settings to indexes failed,msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public void upsertNodeSettings(EsConnConfig connConfig, Map<String, Object> settings) {
        try {
            Map<String, JsonData> settingMap = new HashMap<>();

            String hosts = (String) (settings.get(EsTriggerConstant.TRIGGER_IDX_HOST_KEY));
            if (StringUtils.isNotBlank(hosts)) {
                settingMap.put(EsTriggerConstant.TRIGGER_IDX_HOST_KEY, JsonData.of(hosts));
            }

            String user = (String) (settings.get(EsTriggerConstant.TRIGGER_IDX_USER_KEY));
            if (StringUtils.isNotBlank(user)) {
                settingMap.put(EsTriggerConstant.TRIGGER_IDX_USER_KEY, JsonData.of(user));
            }

            String password = (String) (settings.get(EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY));
            if (StringUtils.isNotBlank(password)) {
                settingMap.put(EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY, JsonData.of(password));
            }

            PutClusterSettingsRequest req = new PutClusterSettingsRequest.Builder().persistent(settingMap).build();

            ElasticsearchClient esClient = Es8ClientHelper.generateEsClient(connConfig);
            PutClusterSettingsResponse res = esClient.cluster().putSettings(req);
            if (!res.acknowledged()) {
                throw new RuntimeException("Update node settings failed, acknowledged is false.");
            }
        } catch (Exception e) {
            String errMsg = "Upsert settings to node,msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public Map<String, Object> queryIndexCcSettings(EsConnConfig connConfig, String idxName, List<String> keys) {
        try {
            GetIndicesSettingsRequest req = new GetIndicesSettingsRequest.Builder().index(idxName).name(keys).build();
            GetIndicesSettingsResponse res = Es8ClientHelper.generateEsClient(connConfig).indices().getSettings(req);
            IndexSettings settings = res.result().get(idxName).settings();
            Map<String, Object> re = new HashMap<>();
            if (settings != null && settings.index() != null) {
                Map<String, JsonData> x = settings.index().otherSettings();
                if (x != null) {
                    for (String key : keys) {
                        String realKey = key.substring(key.lastIndexOf(".") + 1);
                        JsonData d = x.get(realKey);
                        if (d != null) {
                            re.put(key, d.to(String.class));
                        }
                    }
                }
            }

            return re;
        } catch (Exception e) {
            String errMsg = "Upsert settings to node,msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    public Map<String, Object> queryCcNodeSettings(EsConnConfig connConfig) {
        try {
            GetClusterSettingsRequest req = new GetClusterSettingsRequest.Builder().includeDefaults(false).build();
            ElasticsearchClient esClient = Es8ClientHelper.generateEsClient(connConfig);

            GetClusterSettingsResponse res = esClient.cluster().getSettings(req);

            JsonData confJson = res.persistent().get(EsTriggerConstant.NODE_CONF_PREFIX);
            if (confJson == null) {
                return new HashMap<>();
            }

            return confJson.to(Map.class);
        } catch (Exception e) {
            String errMsg = "Get cc node settings failed,msg:" + ExceptionUtils.getRootCauseMessage(e);
            log.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    protected void createIdx(String indexName, TypeMapping typeMapping, EsConnConfig connConfig) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest.Builder().index(indexName).mappings(typeMapping).build();

        ElasticsearchClient esClient = Es8ClientHelper.generateEsClient(connConfig);
        CreateIndexResponse response = esClient.indices().create(request);
        if (!response.acknowledged()) {
            throw new RuntimeException("Index " + indexName + " create failed, acknowledged is false.");
        }
    }
}
