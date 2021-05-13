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

    public ClickHouseSyncService(DataSource dataSource,
                                 Map<String, Map<String, MappingConfig>> mappingConfigCache,
                                 Boolean isTcpMode
    ) {
        this.dataSource = dataSource;
        this.mappingConfigCache = mappingConfigCache;
        this.isTcpMode = isTcpMode;

        ColumnsTypeCache.getInstance().setDataSource(dataSource);
    }

    public void sync(List<Dml> dmls) {
        Map<String, List<Dml>> groupByDmls = dmls.stream().collect(Collectors.groupingBy(this::getConfigKey));

        groupByDmls.forEach((key, groupByDml) -> {
            Map<String, MappingConfig> configMap = mappingConfigCache.get(key);
            try {
                Connection connection = dataSource.getConnection();
                configMap.forEach((fileName, mappingConfig) -> {
                    if (mappingConfig.isSignMode()) {
                        batchInsertInSignMode(groupByDml, mappingConfig, connection);
                    } else {
                        groupByDml.forEach(dml -> {
                            String type = dml.getType();
                            if (type != null && type.equalsIgnoreCase("INSERT")) {
                                batchInsert(dml, mappingConfig, connection);
                            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                                batchUpdate(dml, mappingConfig, connection);
                            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                                batchDelete(dml, mappingConfig, connection);
                            }
                        });
                    }
                });
            } catch (SQLException e) {
                logger.error("SQLException {}", e);
                throw new RuntimeException(e);
            }


        });
    }

    private void batchInsertInSignMode(List<Dml> dmls, MappingConfig mappingConfig, Connection connection) {
        BatchExecutor.doBatchInsertInSignMode(dmls, mappingConfig, connection);
    }

    private void batchInsert(Dml dml, MappingConfig mappingConfig, Connection connection) {
        BatchExecutor.doBatchInsert(dml, mappingConfig, connection);
    }

    private void batchUpdate(Dml dml, MappingConfig mappingConfig, Connection connection) {
        BatchExecutor.doBatchUpdate(dml, mappingConfig, connection);
    }

    private void batchDelete(Dml dml, MappingConfig mappingConfig, Connection connection) {
        BatchExecutor.doBatchDelete(dml, mappingConfig, connection);
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

//    private void batchInsert(List<Dml> dmls, MappingConfig mappingConfig) {
//        Dml firstDml = dmls.stream().findFirst().get();
//
////        Map<String, String> columnsMap = SyncUtil.getColumnsMap(mappingConfig.getDbMapping(), firstDml.getData().get(0));
////        String prepareSql = ClickHouseSqlBuilder.buildInsertSql(mappingConfig, firstDml, true);
//
//        //insert table xx (1,2,3)values(?,?,?)  sign -1
//        try {
//            Connection connection = dataSource.getConnection();
//            PreparedStatement preparedStatement = dataSource.getConnection().prepareStatement(prepareSql);
//
//            Map<String, Integer> ctype = getTargetColumnType(connection, mappingConfig);
//
//
//            for (int i = 0; i < dmls.size(); i++) {
//                Dml dml = dmls.get(i);
//
//                for (int i1 = 0; i1 < dml.getData().size(); i1++) {
//
//                }
//
//                Integer type = ctype.get(Util.cleanColumn(src).toLowerCase());
//                if (type == null) {
//                    throw new RuntimeException("Target column: " + src + " not matched");
//                }
//
//
//                dml.getData().forEach(keyValueMap -> {
//                    SyncUtil.setPStmt(type, preparedStatement, keyValueMap.get(), j);
//
//                });
//
//
//            }
//
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//
//    }


//    private Map<String, Integer> getTargetColumnType(Connection conn, MappingConfig config) {
//        MappingConfig.DbMapping dbMapping = config.getDbMapping();
//        String cacheKey = config.getDestination() + "." + dbMapping.getDatabase() + "." + dbMapping.getTable();
//        Map<String, Integer> columnType = columnsTypeCache.get(cacheKey);
//        if (columnType == null) {
//            synchronized (ClickHouseSyncService.class) {
//                columnType = columnsTypeCache.get(cacheKey);
//                if (columnType == null) {
//                    columnType = new LinkedHashMap<>();
//                    final Map<String, Integer> columnTypeTmp = columnType;
//                    String sql = "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping) + " WHERE 1=2";
//                    Util.sqlRS(conn, sql, rs -> {
//                        try {
//                            ResultSetMetaData rsd = rs.getMetaData();
//                            int columnCount = rsd.getColumnCount();
//                            for (int i = 1; i <= columnCount; i++) {
//                                columnTypeTmp.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
//                            }
//                            columnsTypeCache.put(cacheKey, columnTypeTmp);
//                        } catch (SQLException e) {
//                            logger.error(e.getMessage(), e);
//                        }
//                    });
//                }
//            }
//        }
//        return columnType;
//    }

}
