package com.alibaba.otter.canal.client.adapter.clickhouse;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.sql.builder.SQLBuilder;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.ConfigLoader;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.clickhouse.service.ClickHouseEtlService;
import com.alibaba.otter.canal.client.adapter.clickhouse.service.ClickHouseSyncService;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.ClickHouseTemplate;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiangtiteng on 2021/4/28
 */
@SPI("clickhouse")
public class ClickHouseAdapter implements OuterAdapter {
    private static Logger logger = LoggerFactory.getLogger(ClickHouseAdapter.class);

    private Map<String, MappingConfig> clickHouseMapping = new ConcurrentHashMap<>();  // 文件名对应配置

    private Map<String, Map<String, MappingConfig>> mappingConfigCache = new ConcurrentHashMap<>();  // 库名-表名对应配置

    private Properties properties;

    private boolean isTcpMode;

    private DruidDataSource dataSource;

    private ClickHouseSyncService clickHouseSyncService;

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        this.properties = envProperties;
        this.isTcpMode = "tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"));

        clickHouseMapping = ConfigLoader.load(envProperties, configuration);
        mappingConfigCache = ConfigLoader.buildMappingConfigCache(clickHouseMapping, envProperties);

        initDataSource(configuration.getProperties());

        clickHouseSyncService = new ClickHouseSyncService(this.dataSource, mappingConfigCache, isTcpMode);
    }

    private void initDataSource(Map<String, String> properties) {
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(properties.get("jdbc.driverClassName"));
        dataSource.setUrl(properties.get("jdbc.url"));
        dataSource.setUsername(properties.get("jdbc.username"));
        dataSource.setPassword(properties.get("jdbc.password"));
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(30);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setUseUnfairLock(true);

        try {
            dataSource.init();
        } catch (SQLException e) {
            logger.error("ERROR ## failed to initial datasource: " + properties.get("jdbc.url"), e);
        }
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.size() == 0)
            return;
        clickHouseSyncService.sync(dmls);
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        MappingConfig mappingConfig = clickHouseMapping.get(task);
        ClickHouseTemplate clickHouseTemplate = new ClickHouseTemplate(this.dataSource);
        ClickHouseEtlService clickHouseEtlService = new ClickHouseEtlService(clickHouseTemplate, mappingConfig);

        if (mappingConfig != null) {
            return clickHouseEtlService.importData(params);
        } else {
            EtlResult etlResult = new EtlResult();
            etlResult.setErrorMessage("Don't find the config of " + task);
            return etlResult;
        }
    }

    @Override
    public Map<String, Object> count(String task) {
        MappingConfig mappingConfig = clickHouseMapping.get(task);
        ClickHouseTemplate clickHouseTemplate = new ClickHouseTemplate(this.dataSource);
        Connection conn = clickHouseTemplate.getConn();
        MappingConfig.DbMapping dbMapping = mappingConfig.getDbMapping();
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(String.format("select count() from %s.%s", dbMapping.getTargetDb(), dbMapping.getTargetTable()));
            ResultSet resultSet = preparedStatement.executeQuery();
            long count = resultSet.getLong(1);
            return new HashMap<String, Object>() {{
                put("clickhouseTable", dbMapping.getTargetTable());
                put("count", count);
            }};
        } catch (SQLException e) {
            logger.error("Query count error ", e);
            throw new RuntimeException("Query count error!");
        }
    }

    @Override
    public void destroy() {
    }
}
