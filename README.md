## Intro

The ElasticSearch plugin for capture data change.

Now support ElasticSearch 7.x and 8.x .

## Quick Start

- Get into product root directory.

- Modify `gradle.properties`, change the version corresponding the target ElasticSearch cluster.

  ```sql
  cc.es7.version=7.10.1
  cc.es8.version=8.15.0
  ```
  > Tips: Only need change one of them.

- Build the plugin.

  ```shell
  # for es 7
  sh ./all_build.sh es-trigger-es7
  
  # for es 8
  sh ./all_build.sh es-trigger-es8
  ```
  > Tips: Sub-projects need different version of JAVA.Install and export JAVA_HOME if necessary.

- Copy cloudcanal-es-trigger to ElasticSearch plugins directory.
  
  ```shell
  # for es 7
  scp ${project_root}/es-trigger-es7/build/dist/es-trigger-es7.zip es@127.0.0.1:${es_home}/plugins
  
  # for es 8
  scp ${project_root}/es-trigger-es8/build/dist/es-trigger-es8.zip es@127.0.0.1:${es_home}/plugins
  ```

- Unzip the plugin.

  ```shell
  # for es 7
  unzip es-trigger-es7.zip -d ./es-trigger-es7
  
  # for es 8
  unzip es-trigger-es8.zip -d ./es-trigger-es8
  ```
  
- Restart ElasticSearch and have fun.
