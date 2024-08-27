package com.clougence.cloudcanal.es7.trigger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es7.trigger.ds.Es7ClientConn;
import com.clougence.cloudcanal.es7.trigger.writer.CcEs7TriggerIdxWriterImpl;
import com.clougence.cloudcanal.es_base.CcEsTriggerIdxWriter;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

/**
 * @author bucketli 2024/6/26 18:38:57
 */
public class CcEs7IdxTriggerPlugin extends Plugin {

    private static final Logger         log                = LoggerFactory.getLogger(CcEs7IdxTriggerPlugin.class);

    private final List<Setting<?>>      settings           = new ArrayList<>();

    private final Setting<Boolean>      cdcEnableSetting   = Setting
        .boolSetting(EsTriggerConstant.IDX_ENABLE_CDC_CONF_KEY, false, Setting.Property.IndexScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxMaxScn   = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY, Setting.Property.IndexScope, Setting.Property.Dynamic);

    public static final Setting<String> triggerIdxHost     = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_HOST_KEY, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxUser     = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_USER_KEY, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxPassword = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private CcEsTriggerIdxWriter        triggerIdxWriter;

    public CcEs7IdxTriggerPlugin(){
        settings.add(cdcEnableSetting);
        settings.add(triggerIdxHost);
        settings.add(triggerIdxUser);
        settings.add(triggerIdxPassword);
        settings.add(triggerIdxMaxScn);
    }

    @Override
    public List<Setting<?>> getSettings() { return settings; }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (indexModule.getIndex().getName().equals(EsTriggerConstant.ES_TRIGGER_IDX)) {
            log.info("Not subscribe " + EsTriggerConstant.ES_TRIGGER_IDX);
            return;
        }

        log.info("Add index listener,index:" + indexModule.getIndex());

        final CcEs7IdxOpListener cdcListener = new CcEs7IdxOpListener(indexModule, triggerIdxWriter);
        cdcListener.start();

        indexModule.addSettingsUpdateConsumer(cdcEnableSetting, cdcListener);
        indexModule.addIndexOperationListener(cdcListener);
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool, ResourceWatcherService resourceWatcherService,
                                               ScriptService scriptService, NamedXContentRegistry xContentRegistry, Environment environment, NodeEnvironment nodeEnvironment,
                                               NamedWriteableRegistry namedWriteableRegistry, IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        log.info(this.getClass().getSimpleName() + " createComponents");
        try {
            Es7ClientConn.instance.addHostSettingConsumer(clusterService.getClusterSettings());
            initIdxWriter();
        } catch (Exception e) {
            // not throw, or will make ElasticSearch node fail.
            log.error("Create components FAILED,but ignore.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }

        return super.createComponents(client, clusterService, threadPool, resourceWatcherService, scriptService, xContentRegistry, environment, nodeEnvironment, namedWriteableRegistry, indexNameExpressionResolver, repositoriesServiceSupplier);
    }

    protected synchronized void initIdxWriter() {
        if (triggerIdxWriter == null) {
            triggerIdxWriter = new CcEs7TriggerIdxWriterImpl();
            triggerIdxWriter.start();
        }
    }
}
