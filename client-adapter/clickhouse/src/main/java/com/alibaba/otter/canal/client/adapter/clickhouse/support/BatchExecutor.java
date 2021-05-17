package com.alibaba.otter.canal.client.adapter.clickhouse.support;

import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jiangtiteng on 2021/5/13
 */
public class BatchExecutor {
    private static Logger logger = LoggerFactory.getLogger(BatchExecutor.class);

    private DataSource dataSource;

    public BatchExecutor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private Connection conn;

    public Connection getConn() {
        if (conn == null) {
            try {
                conn = dataSource.getConnection();
                this.conn.setAutoCommit(false);
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
        return conn;
    }

    private List<String> getColumnsFromDMl(Dml dml) {
        String type = dml.getType();
        if (type != null && type.equalsIgnoreCase("INSERT")) {
            Map<String, Object> valueMap = dml.getData().get(0);
            return new ArrayList<>(valueMap.keySet());
        } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
            Map<String, Object> valueMap = dml.getOld().get(0);
            return new ArrayList<>(valueMap.keySet());
        } else {
            return new ArrayList<>();
        }
    }

    public void doBatchUpdate(Dml dml, MappingConfig mappingConfig) {
        Connection connection = getConn();
        List<String> columnKeys = getColumnsFromDMl(dml);
        List<String> pkNames = dml.getPkNames();
        Map<String, Integer> ctype = ColumnsTypeCache.getInstance().getColumnsTypeByMappingConfig(mappingConfig, conn);

        ClickHouseSqlBuilder clickHouseSqlBuilder = new ClickHouseSqlBuilder()
                .setType(ClickHouseSqlBuilder.SqlType.SQL_TYPE_UPDATE)
                .setMapAll(mappingConfig.getDbMapping().getMapAll())
                .setDbAndTable(mappingConfig.getDbMapping().getTargetDb(), mappingConfig.getDbMapping().getTable())
                .setColumnsMap(mappingConfig.getDbMapping().getTargetColumns())
                .setPkNames(pkNames)
                .setColumns(columnKeys);

        String prepareUpdateSql = clickHouseSqlBuilder.build();

        try {
            PreparedStatement preparedUpdateStatement = connection.prepareStatement(prepareUpdateSql);
            for (Map<String, Object> data : dml.getData()) {
                int i = 0;
                for (; i < columnKeys.size(); i++) {
                    String columnKey = columnKeys.get(i);
                    SyncUtil.setPStmt(ctype.get(columnKey),
                            preparedUpdateStatement,
                            data.get(columnKey)
                            , i + 1);
                }

                for (int j = 0; j < pkNames.size(); j++) {
                    String columnKey = pkNames.get(j);
                    SyncUtil.setPStmt(ctype.get(columnKey),
                            preparedUpdateStatement,
                            data.get(columnKey)
                            , j + i + 1);
                }
                preparedUpdateStatement.addBatch();
            }
            preparedUpdateStatement.executeBatch();
        } catch (SQLException e) {
            logger.error("doBatchUpdate error", e);
            throw new RuntimeException(e);
        }
    }

    public void doBatchDelete(Dml dml, MappingConfig mappingConfig) {
        Connection connection = getConn();
        Map<String, Integer> ctype = ColumnsTypeCache.getInstance().getColumnsTypeByMappingConfig(mappingConfig, conn);
        List<String> pkNames = dml.getPkNames();

        ClickHouseSqlBuilder clickHouseSqlBuilder = new ClickHouseSqlBuilder()
                .setType(ClickHouseSqlBuilder.SqlType.SQL_TYPE_DELETE)
                .setMapAll(mappingConfig.getDbMapping().getMapAll())
                .setDbAndTable(mappingConfig.getDbMapping().getTargetDb(), mappingConfig.getDbMapping().getTable())
                .setColumnsMap(mappingConfig.getDbMapping().getTargetColumns())
                .setPkNames(pkNames);
        String prepareDeleteSql = clickHouseSqlBuilder.build();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(prepareDeleteSql);

            for (Map<String, Object> data : dml.getData()) {
                for (int i = 0; i <= pkNames.size(); i++) {
                    String columnKey = pkNames.get(i);
                    SyncUtil.setPStmt(ctype.get(columnKey),
                            preparedStatement,
                            data.get(columnKey)
                            , i + 1);
                }
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            logger.error("doBatchDelete error", e);
            throw new RuntimeException(e);
        }
    }

    public void doBatchInsert(Dml dml, MappingConfig mappingConfig) {
        Connection connection = getConn();
        List<String> columnKeys = getColumnsFromDMl(dml);
        Map<String, Integer> ctype = ColumnsTypeCache.getInstance().getColumnsTypeByMappingConfig(mappingConfig, conn);

        ClickHouseSqlBuilder clickHouseSqlBuilder = new ClickHouseSqlBuilder()
                .setType(ClickHouseSqlBuilder.SqlType.SQL_TYPE_INSERT)
                .setMapAll(mappingConfig.getDbMapping().getMapAll())
                .setColumns(columnKeys)
                .setDbAndTable(mappingConfig.getDbMapping().getTargetDb(), mappingConfig.getDbMapping().getTable())
                .setColumnsMap(mappingConfig.getDbMapping().getTargetColumns());
        String prepareInsertSql = clickHouseSqlBuilder.build();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(prepareInsertSql);
            addBatchForInsertWithSign(ctype, columnKeys, preparedStatement,
                    ClickHouseSqlBuilder.SqlType.SQL_TYPE_INSERT, dml, 1, false);
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            logger.error("doBatchInsert error", e);
            throw new RuntimeException(e);
        }
    }

    public void doBatchInsertInSignMode(List<Dml> dmls, MappingConfig mappingConfig) {

        Connection connection = getConn();
        List<String> columnKeys = getColumnsFromDMl(dmls.get(0));
        Map<String, Integer> ctype = ColumnsTypeCache.getInstance().getColumnsTypeByMappingConfig(mappingConfig, conn);

        ClickHouseSqlBuilder clickHouseSqlBuilder = new ClickHouseSqlBuilder()
                .setType(ClickHouseSqlBuilder.SqlType.SQL_TYPE_INSERT)
                .setMapAll(mappingConfig.getDbMapping().getMapAll())
                .setSignKey(mappingConfig.getDbMapping().getSignKey())
                .setSign(true)
                .setColumns(columnKeys)
                .setDbAndTable(mappingConfig.getDbMapping().getTargetDb(), mappingConfig.getDbMapping().getTable())
                .setColumnsMap(mappingConfig.getDbMapping().getTargetColumns());
        String prepareInsertForSignSql = clickHouseSqlBuilder.build();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(prepareInsertForSignSql);

            for (Dml dml : dmls) {
                if (dml.getType() != null && dml.getType().equalsIgnoreCase("INSERT")) {
                    addBatchForInsertWithSign(ctype, columnKeys, preparedStatement,
                            ClickHouseSqlBuilder.SqlType.SQL_TYPE_INSERT, dml, 1, true);
                } else if (dml.getType() != null && dml.getType().equalsIgnoreCase("UPDATE")) {
                    addBatchForInsertWithSign(ctype, columnKeys, preparedStatement,
                            ClickHouseSqlBuilder.SqlType.SQL_TYPE_INSERT, dml, -1, true);
                    addBatchForInsertWithSign(ctype, columnKeys, preparedStatement,
                            ClickHouseSqlBuilder.SqlType.SQL_TYPE_INSERT, dml, 1, true);
                } else if (dml.getType() != null && dml.getType().equalsIgnoreCase("DELETE")) {
                    addBatchForInsertWithSign(ctype, columnKeys, preparedStatement,
                            ClickHouseSqlBuilder.SqlType.SQL_TYPE_INSERT, dml, -1, true);
                }
            }
            preparedStatement.executeBatch();
        } catch (SQLException e) {
            logger.error("doBatchInsertInSignMode error", e);
            throw new RuntimeException(e);
        }
    }


    private void addBatchForInsertWithSign(Map<String, Integer> ctype,
                                           List<String> columnKeys,
                                           PreparedStatement preparedStatement,
                                           ClickHouseSqlBuilder.SqlType sqlType,
                                           Dml dml,
                                           int sign,
                                           boolean isSignMode

    ) throws SQLException {
        for (Map<String, Object> datum : dml.getData()) {
            int i = 0;
            for (; i < columnKeys.size(); i++) {
                String columnKey = columnKeys.get(i);
                SyncUtil.setPStmt(ctype.get(columnKey),
                        preparedStatement,
                        datum.get(columnKey)
                        , i + 1);
            }
            if (isSignMode) {
                SyncUtil.setPStmt(Types.INTEGER, preparedStatement, sign, i + 1);
            }
            preparedStatement.addBatch();
        }
    }
}
