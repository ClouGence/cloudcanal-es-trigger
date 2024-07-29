# cloudcanal-es-trigger
The ElasticSearch plugin for capture data change.

## Quick Start

- In product root directory.

  ```shell
  sh ./all_build.sh
  ```

- Copy cloudcanal-es-trigger to ElasticSearch plugins directory.
  
  ```shell
  scp ${project_root}/es-trigger-es78/build/dist/es-trigger-es78.zip es@127.0.0.1:${es_home}/plugins
  ```

- Unzip the plugin.

  ```shell
  unzip es-trigger-es78.zip -d ./es-trigger-es78
  ```

- Add logger config.
  
  ```shell
  vi ${es_home}/config/log4j2.properties
  ```
  
  > logger.ccplugin.name = com.clougence.cloudcanal.es78.trigger.CcEsIdxOpListener
  > 
  > logger.ccplugin.level = info
  
- Restart ElasticSearch and have fun.
