package com.alibaba.otter.canal.client.adapter.clickhouse.support;

import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiangtiteng on 2021/5/13
 */
public class ColumnsTypeCache {
    private static ColumnsTypeCache columnsTypeCache = new ColumnsTypeCache();

    private Map<String, Map<String, Integer>> columnsTypeCacheMap;

    private static final Logger logger = LoggerFactory.getLogger(ColumnsTypeCache.class);

    public static ColumnsTypeCache getInstance() {
        return columnsTypeCache;
    }

    private ColumnsTypeCache() {
        columnsTypeCacheMap = new ConcurrentHashMap<>();
    }

    /**
     * 获取对应表字段的数据类型
     *
     * @param config
     * @return 字段名->字段类型
     */
    public Map<String, Integer> getColumnsTypeByMappingConfig(MappingConfig config, Connection conn) {
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        String cacheKey = config.getDestination() + "." + dbMapping.getDatabase() + "." + dbMapping.getTable();

        Map<String, Integer> columnType = columnsTypeCacheMap.get(cacheKey);
        if (columnType == null) {
            synchronized (cacheKey.intern()) {
                if (columnsTypeCacheMap.get(cacheKey) == null) {
                    columnType = new LinkedHashMap<>();
                    final Map<String, Integer> columnTypeTmp = columnType;
                    String sql = "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping) + " WHERE 1=2";
                    Util.sqlRS(conn, sql, rs -> {
                        try {
                            ResultSetMetaData rsd = rs.getMetaData();
                            int columnCount = rsd.getColumnCount();
                            for (int i = 1; i <= columnCount; i++) {
                                columnTypeTmp.put(rsd.getColumnName(i), rsd.getColumnType(i));
                            }
                            columnsTypeCacheMap.put(cacheKey, columnTypeTmp);
                        } catch (SQLException e) {
                            logger.error(e.getMessage(), e);
                        }
                    });
                } else {
                    columnType = columnsTypeCacheMap.get(cacheKey);
                }
            }
        }
        return columnType;
    }
}
