package com.alibaba.otter.canal.client.adapter.clickhouse.support;

import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;

import java.util.List;
import java.util.Map;

/**
 * Created by jiangtiteng on 2021/5/12
 */
public class ClickHouseSqlBuilder {
    public static enum SqlType {
        SQL_TYPE_INSERT,
        SQL_TYPE_UPDATE,
        SQL_TYPE_DELETE
    }

    /**
     * sql类型
     */
    private SqlType sqlType;

    /**
     * 是否映射所有字段，为true时忽略columnsMap
     */
    private Boolean mapAll;

    /**
     * 字段名映射
     */
    private Map<String, String> columnsMap;

    /**
     * 库名
     */
    private String db;

    /**
     * 表名
     */
    private String tableName;

    /**
     * sign模式key，默认为sign
     */
    private String signKey;

    public ClickHouseSqlBuilder setType(SqlType sqlType) {
        this.sqlType = sqlType;
        return this;
    }

    public ClickHouseSqlBuilder setMapAll(Boolean mapAll) {
        this.mapAll = mapAll;
        return this;
    }

    public ClickHouseSqlBuilder setColumnsMap(Map<String, String> columnsMap) {
        this.columnsMap = columnsMap;
        return this;
    }

    public ClickHouseSqlBuilder setDbAndTable(String db, String tableName) {
        this.db = db;
        this.tableName = tableName;
        return this;
    }

    public ClickHouseSqlBuilder setSignKey(String signKey) {
        this.signKey = signKey;
        return this;
    }

    public String build() {
        return null;
    }

    public String buildForSignMode() {
        return null;
    }


}
