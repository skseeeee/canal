package com.alibaba.otter.canal.client.adapter.clickhouse;

import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiangtiteng on 2021/4/28
 */
public class ClickHouseAdapter implements OuterAdapter {

    private Map<String, MappingConfig> clickHouseMapping = new ConcurrentHashMap<>();  // 文件名对应配置
    private Map<String, Map<String, MappingConfig>> mappingConfigCache = new ConcurrentHashMap<>();  // 库名-表名对应配置

    private Properties properties;

    private boolean isTcpMode;

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        this.properties = envProperties;
        this.isTcpMode = "tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"));

        clickHouseMapping = ConfigLoader.load(envProperties, configuration);
        mappingConfigCache = ConfigLoader.buildMappingConfigCache(clickHouseMapping, envProperties);
        //Map<String, MappingConfig>

    }

    @Override
    public void sync(List<Dml> dmls) {


    }

    @Override
    public void destroy() {

    }
}
