package com.alibaba.otter.canal.client.adapter.clickhouse.config;

import com.alibaba.otter.canal.client.adapter.config.YmlConfigBinder;
import com.alibaba.otter.canal.client.adapter.support.MappingConfigsLoader;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by jiangtiteng on 2021/4/29
 */
public class ConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    public static Map<String, MappingConfig> load(Properties envProperties, OuterAdapterConfig configuration) {
        logger.info("## Start loading rdb mapping config ... ");

        Map<String, MappingConfig> result = new LinkedHashMap<>();

        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs("clickhouse");
        configContentMap.forEach((fileName, content) -> {
            MappingConfig config = YmlConfigBinder
                    .bindYmlToObj(null, content, MappingConfig.class, null, envProperties);
            if (config == null) {
                return;
            }
            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + fileName + " " + e.getMessage(), e);
            }
            result.put(fileName, config);
        });


        logger.info("## Rdb mapping config loaded");
        return filterInvalidMappingConfig(result, configuration);
    }

    private static Map<String, MappingConfig> filterInvalidMappingConfig(Map<String, MappingConfig> mappingConfigMap, OuterAdapterConfig configuration) {
        return mappingConfigMap.entrySet().stream().filter(stringMappingConfigEntry -> {

            MappingConfig mappingConfig = stringMappingConfigEntry.getValue();
            return ((!StringUtils.isNotEmpty(mappingConfig.getOuterAdapterKey()) &&
                    !StringUtils.isNotEmpty(configuration.getKey())) ||
                    (StringUtils.isNotEmpty(mappingConfig.getOuterAdapterKey()) &&
                            mappingConfig.getOuterAdapterKey().equalsIgnoreCase(configuration.getKey()))
            );
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<String, Map<String, MappingConfig>> buildMappingConfigCache(Map<String, MappingConfig> clickHouseMapping, Properties envProperties) {
        Map<String, Map<String, MappingConfig>> mappingConfigCache = new ConcurrentHashMap<>();

        for (Map.Entry<String, MappingConfig> entry : clickHouseMapping.entrySet()) {
            String configName = entry.getKey();
            MappingConfig mappingConfig = entry.getValue();
            String k;
            if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "-"
                        + StringUtils.trimToEmpty(mappingConfig.getGroupId()) + "_"
                        + mappingConfig.getMapping().getDatabase() + "-" + mappingConfig.getMapping().getTable();
            } else {
                k = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_"
                        + mappingConfig.getMapping().getDatabase() + "_" + mappingConfig.getMapping().getTable();
            }
            Map<String, MappingConfig> configMap = mappingConfigCache.computeIfAbsent(k,
                    k1 -> new ConcurrentHashMap<>());
            configMap.put(configName, mappingConfig);
        }
        return mappingConfigCache;
    }
}
