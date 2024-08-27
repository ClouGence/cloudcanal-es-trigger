package com.clougence.cloudcanal.es_base;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author bucketli 2024/7/30 14:15:44
 */
public interface CcMetaService {

    void createTriggerIdx(EsConnConfig connConfig);

    void upsertIndexSettings(EsConnConfig connConfig, List<String> indexes, Map<String, Object> settings);

    void upsertNodeSettings(EsConnConfig connConfig, Map<String, Object> settings);

    /**
     * Not for standard settings
     */
    Map<String, Object> queryIndexCcSettings(EsConnConfig connConfig, String idxName, List<String> keys);

    Map<String, Object> queryCcNodeSettings(EsConnConfig connConfig);
}
