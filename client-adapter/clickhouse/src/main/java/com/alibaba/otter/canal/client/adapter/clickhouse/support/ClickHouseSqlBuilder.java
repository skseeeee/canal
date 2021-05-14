package com.alibaba.otter.canal.client.adapter.clickhouse.support;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by jiangtiteng on 2021/5/12
 */
public class ClickHouseSqlBuilder {
    public static enum SqlType {
        SQL_TYPE_INSERT {
            @Override
            public String spliceSql(ClickHouseSqlBuilder c) {

                if (c.isSign) {
                    c.columns.add(c.signKey);
                }

                String fields = c.columns.stream().map(x -> c.reverseColumnsMap.getOrDefault(x, c.signKey)).collect(Collectors.joining(","));

                String placeholder = c.columns.stream().map(x -> "?").collect(Collectors.joining(","));

                return String.format("insert into %s.%s(%s) values (%s)", c.db, c.tableName, fields, placeholder);

            }
        },
        SQL_TYPE_UPDATE {
            @Override
            public String spliceSql(ClickHouseSqlBuilder c) {

                String updateFields = c.columns.stream().map(x -> String.format("%s = ?", c.reverseColumnsMap.get(x))).collect(Collectors.joining(","));

                return String.format("alter table %s.%s update %s where %s=?", c.db, c.tableName, updateFields, c.pkNames);
            }
        },
        SQL_TYPE_DELETE {
            @Override
            public String spliceSql(ClickHouseSqlBuilder c) {
                return String.format("alter table %s.%s delete where %s=?", c.db, c.tableName, c.pkNames);
            }
        };

        public abstract String spliceSql(ClickHouseSqlBuilder c);


    }

    /**
     * sql类型
     */
    private SqlType sqlType;

    /**
     * 是否映射所有字段，为true时忽略columnsMap
     */
    private Boolean mapAll = false;

    /**
     * 字段映射 目标:源
     */
    private Map<String, String> columnsMap;

    /**
     * 字段映射 源:目标
     */
    private Map<String, String> reverseColumnsMap;

    /**
     * 字段名
     */
    private List<String> columns;

    /**
     * 主键名
     */
    private String pkNames;

    /**
     * 库名
     */
    private String db;

    /**
     * 表名
     */
    private String tableName;

    /**
     * sign模式key，默认为_sign
     */
    private String signKey = "_sign";

    /**
     * 是否是标记模式
     */
    private Boolean isSign = false;


    public ClickHouseSqlBuilder setType(SqlType sqlType) {
        this.sqlType = sqlType;
        return this;
    }

    public ClickHouseSqlBuilder setMapAll(Boolean mapAll) {
        this.mapAll = mapAll;
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

    public ClickHouseSqlBuilder setColumns(List<String> columns) {
        this.columns = columns;
        return this;
    }


    public ClickHouseSqlBuilder setPkNames(String pkNames) {
        this.pkNames = pkNames;
        return this;
    }


    public ClickHouseSqlBuilder setSign(Boolean sign) {
        isSign = sign;
        return this;
    }

    public ClickHouseSqlBuilder setColumnsMap(Map<String, String> columnsMap) {
        this.columnsMap = columnsMap;
        HashMap<String, String> rev = new HashMap<>();
        for (Map.Entry<String, String> entry : columnsMap.entrySet())
            rev.put(entry.getValue(), entry.getKey());
        this.reverseColumnsMap = rev;
        return this;
    }

    public String build() {

        if (mapAll) {
            this.reverseColumnsMap = this.columns.stream().collect(Collectors.toMap(x -> x, x -> x));
        }

        if (isSign) {
            return SqlType.SQL_TYPE_INSERT.spliceSql(this);
        } else {
            return sqlType.spliceSql(this);
        }
    }

}
