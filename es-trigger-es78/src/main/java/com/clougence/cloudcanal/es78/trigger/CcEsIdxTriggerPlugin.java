package com.clougence.cloudcanal.es78.trigger;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.Plugin;

/**
 * @author bucketli 2024/6/26 18:38:57
 */
public class CcEsIdxTriggerPlugin extends Plugin {

    private final List<Setting<?>> settings         = new ArrayList<>();

    private final Setting<Boolean> cdcEnableSetting = Setting.boolSetting(CcEsIdxOpListener.IDX_ENABLE_CDC_CONF_KEY, false, Setting.Property.IndexScope, Setting.Property.Dynamic);

    public CcEsIdxTriggerPlugin(){
        settings.add(cdcEnableSetting);
    }

    @Override
    public List<Setting<?>> getSettings() { return settings; }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        final CcEsIdxOpListener cdcListener = new CcEsIdxOpListener(indexModule);
        indexModule.addSettingsUpdateConsumer(cdcEnableSetting, cdcListener);
        indexModule.addIndexOperationListener(cdcListener);
    }
}
