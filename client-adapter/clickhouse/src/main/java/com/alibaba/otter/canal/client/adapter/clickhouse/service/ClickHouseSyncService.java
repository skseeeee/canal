package com.alibaba.otter.canal.client.adapter.clickhouse.service;

import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.BatchExecutor;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.ClickHouseSqlBuilder;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.ColumnsTypeCache;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.SyncUtil;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by jiangtiteng on 2021/5/7
 */
public class ClickHouseSyncService {
    private DataSource dataSource;

    private Map<String, Map<String, MappingConfig>> mappingConfigCache;

    private Boolean isTcpMode;

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSyncService.class);

    private BatchExecutor batchExecutor;

    public ClickHouseSyncService(DataSource dataSource,
                                 Map<String, Map<String, MappingConfig>> mappingConfigCache,
                                 Boolean isTcpMode
    ) {
        this.dataSource = dataSource;
        this.mappingConfigCache = mappingConfigCache;
        this.isTcpMode = isTcpMode;
        this.batchExecutor = new BatchExecutor(dataSource);
    }

    public void sync(List<Dml> dmls) {
        Map<String, List<Dml>> groupByDmls = dmls.stream().collect(Collectors.groupingBy(this::getConfigKey));

        groupByDmls.forEach((key, groupByDml) -> {
            Map<String, MappingConfig> configMap = mappingConfigCache.get(key);
            configMap.forEach((fileName, mappingConfig) -> {
                if (mappingConfig.isSignMode()) {
                    batchInsertInSignMode(groupByDml, mappingConfig);
                } else {
                    groupByDml.forEach(dml -> {
                        String type = dml.getType();
                        if (type != null && type.equalsIgnoreCase("INSERT")) {
                            batchInsert(dml, mappingConfig);
                        } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                            batchUpdate(dml, mappingConfig);
                        } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                            batchDelete(dml, mappingConfig);
                        }
                    });
                }
            });
        });
    }

    private void batchInsertInSignMode(List<Dml> dmls, MappingConfig mappingConfig) {
        batchExecutor.doBatchInsertInSignMode(dmls, mappingConfig);
    }

    private void batchInsert(Dml dml, MappingConfig mappingConfig) {
        batchExecutor.doBatchInsert(dml, mappingConfig);
    }

    private void batchUpdate(Dml dml, MappingConfig mappingConfig) {
        batchExecutor.doBatchUpdate(dml, mappingConfig);
    }

    private void batchDelete(Dml dml, MappingConfig mappingConfig) {
        batchExecutor.doBatchDelete(dml, mappingConfig);
    }

    private String getConfigKey(Dml dml) {
        String destination = StringUtils.trimToEmpty(dml.getDestination());
        String groupId = StringUtils.trimToEmpty(dml.getGroupId());
        String database = dml.getDatabase();
        String table = dml.getTable();

        if (!isTcpMode) {
            return destination + "_" + groupId + "_" + database + "_" + table;
        } else {
            return destination + "_" + database + "_" + table;
        }
    }
}
