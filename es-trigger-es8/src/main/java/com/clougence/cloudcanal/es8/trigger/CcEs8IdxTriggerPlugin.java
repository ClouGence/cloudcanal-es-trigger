package com.clougence.cloudcanal.es8.trigger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.es8.trigger.ds.Es8ClientConn;
import com.clougence.cloudcanal.es8.trigger.writer.CcEs8TriggerIdxWriterImpl;
import com.clougence.cloudcanal.es_base.CcEsTriggerIdxWriter;
import com.clougence.cloudcanal.es_base.ComponentLifeCycle;
import com.clougence.cloudcanal.es_base.EsTriggerConstant;

/**
 * @author bucketli 2024/6/26 18:38:57
 */
public class CcEs8IdxTriggerPlugin extends Plugin {

    private static final Logger         log                = LoggerFactory.getLogger(CcEs8IdxTriggerPlugin.class);

    private final List<Setting<?>>      settings           = new ArrayList<>();

    private final Setting<Boolean>      cdcEnableSetting   = Setting
        .boolSetting(EsTriggerConstant.IDX_ENABLE_CDC_CONF_KEY, false, Setting.Property.IndexScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxMaxScn   = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_MAX_SCN_KEY, Setting.Property.IndexScope, Setting.Property.Dynamic);

    public static final Setting<String> triggerIdxHost     = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_HOST_KEY, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxUser     = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_USER_KEY, Setting.Property.NodeScope, Setting.Property.Dynamic);
    public static final Setting<String> triggerIdxPassword = Setting.simpleString(EsTriggerConstant.TRIGGER_IDX_PASSWD_KEY, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private CcEsTriggerIdxWriter        triggerIdxWriter;

    public CcEs8IdxTriggerPlugin(){
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

        final CcEs8IdxOpListener cdcListener = new CcEs8IdxOpListener(indexModule, triggerIdxWriter);
        cdcListener.start();

        indexModule.addSettingsUpdateConsumer(cdcEnableSetting, cdcListener);
        indexModule.addIndexOperationListener(cdcListener);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        log.info(this.getClass().getSimpleName() + " createComponents");
        Es8ClientConn.instance.addHostSettingConsumer(services.clusterService().getClusterSettings());
        initIdxWriter();
        return super.createComponents(services);
    }

    protected synchronized void initIdxWriter() {
        if (triggerIdxWriter == null) {
            triggerIdxWriter = new CcEs8TriggerIdxWriterImpl();
            ((ComponentLifeCycle) triggerIdxWriter).start();
        }
    }
}
