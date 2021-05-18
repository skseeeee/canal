package com.alibaba.otter.canal.client.adapter.clickhouse.service;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.clickhouse.support.BaseExecutor;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.google.common.collect.Lists;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by mew on 2021/5/7
 **/
public class ClickHouseEtlService {
    private static Logger logger = LoggerFactory.getLogger(ClickHouseEtlService.class);

    private BaseExecutor clickHouseTemplate;

    private MappingConfig config;

    private static final String ETL_SQL = "insert into %s.%s (%s) select %s from mysql('%s','%s','%s','%s','%s') where %s";

    public ClickHouseEtlService(BaseExecutor clickHouseTemplate, MappingConfig config) {
        this.clickHouseTemplate = clickHouseTemplate;
        this.config = config;
    }

    public EtlResult importData(List<String> params) {
        EtlResult etlResult = new EtlResult();
        Connection conn = clickHouseTemplate.getConn();
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        Pair<String, String> columnsMap = getColumnsMap(dbMapping);
        String etlSql = String.format(
                ETL_SQL,
                dbMapping.getTargetDb(),
                dbMapping.getTargetTable(),
                columnsMap.getKey(),
                columnsMap.getValue(),
                getSrcDsHostPort(dataSource.getUrl()),
                dbMapping.getDatabase(),
                dbMapping.getTable(),
                dataSource.getUsername(),
                dataSource.getPassword(),
                parseParams(params)
        );
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(etlSql);
            preparedStatement.execute();
            etlResult.setSucceeded(true);
        } catch (SQLException e) {
            logger.error("clickhosue etl error sql: {}", etlSql, e);
            etlResult.setSucceeded(false);
            etlResult.setResultMessage("clickhosue etl error => " + e.getMessage());
            throw new RuntimeException(e);
        }
        return etlResult;
    }

    private Pair<String, String> getColumnsMap(MappingConfig.DbMapping dbMapping) {
        if (dbMapping.getMapAll()) {
            return new Pair<>("*", "*");
        } else if (!CollectionUtils.isEmpty(dbMapping.getAllMapColumns())) {
            ArrayList<String> targetColumns = Lists.newArrayList();
            ArrayList<String> sourceColumns = Lists.newArrayList();
            for (Map.Entry<String, String> entry : dbMapping.getAllMapColumns().entrySet()) {
                targetColumns.add(entry.getKey());
                sourceColumns.add(entry.getValue());
            }
            return new Pair<>(String.join(",", targetColumns), String.join(",", sourceColumns));
        } else {
            throw new RuntimeException("The value of AllMapColumns couldn't be empty if MapAll is true");
        }
    }

    private String parseParams(List<String> params) {
        if (CollectionUtils.isEmpty(params)) {
            return "1=1";
        } else {
            String where = params.stream().filter(x -> x.contains("=")).collect(Collectors.joining("and"));
            return StringUtils.isEmpty(where) ? "1=1" : where;
        }
    }

    private String getSrcDsHostPort(String JDBCUrl) {
        Pattern p = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+)\\:(\\d+)");
        Matcher m = p.matcher(JDBCUrl);
        if (m.find()) {
            return String.format("%s:%s", m.group(1), m.group(2));
        } else {
            return "";
        }
    }

}
