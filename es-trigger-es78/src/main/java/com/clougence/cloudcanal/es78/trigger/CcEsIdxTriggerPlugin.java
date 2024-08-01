package com.clougence.cloudcanal.es78.trigger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

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

import com.clougence.cloudcanal.es78.trigger.ds.Es7ClientConn;
import com.clougence.cloudcanal.es78.trigger.writer.CcEsTriggerIdxWriter;
import com.clougence.cloudcanal.es78.trigger.writer.CcEsTriggerIdxWriterImpl;
import com.clougence.cloudcanal.es_base.ComponentLifeCycle;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

/**
 * @author bucketli 2024/6/26 18:38:57
 */
public class CcEsIdxTriggerPlugin extends Plugin {

    private static final Logger         log                = LoggerFactory.getLogger(CcEsIdxTriggerPlugin.class);

    private final List<Setting<?>>      settings           = new ArrayList<>();

    private final Setting<Boolean>      cdcEnableSetting   = Setting
        .boolSetting(EsTriggerConstant.IDX_ENABLE_CDC_CONF_KEY, false, Setting.Property.IndexScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxMaxScn   = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY, Setting.Property.IndexScope, Setting.Property.Dynamic);

    public static final Setting<String> triggerIdxHost     = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_HOST_KEY, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxUser     = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_USER_KEY, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxPassword = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private CcEsTriggerIdxWriter        triggerIdxWriter;

    public CcEsIdxTriggerPlugin(){
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
        log.info("Add index listener,index:" + indexModule.getIndex());

        final CcEsIdxOpListener cdcListener = new CcEsIdxOpListener(indexModule, triggerIdxWriter);
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
        Es7ClientConn.instance.addHostSettingConsumer(clusterService.getClusterSettings());
        initIdxWriter();
        return super.createComponents(client, clusterService, threadPool, resourceWatcherService, scriptService, xContentRegistry, environment, nodeEnvironment, namedWriteableRegistry, indexNameExpressionResolver, repositoriesServiceSupplier);
    }

    protected synchronized void initIdxWriter() {
        if (triggerIdxWriter == null) {
            triggerIdxWriter = new CcEsTriggerIdxWriterImpl();
            ((ComponentLifeCycle) triggerIdxWriter).start();
        }
    }
}
