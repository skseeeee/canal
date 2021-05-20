package com.alibaba.otter.canal.client.adapter.clickhouse.config;

import com.alibaba.otter.canal.client.adapter.support.AdapterConfig;

import java.util.Map;

/**
 * Created by jiangtiteng on 2021/4/29
 */
public class MappingConfig implements AdapterConfig {
    //数据源key
    private String dataSourceKey;

    //canal实例或MQ的topic
    private String destination;

    //groupId
    private String groupId;

    //对应适配器的key
    private String outerAdapterKey;

    //db映射配置
    private DbMapping dbMapping;

    //sign模式
    private boolean signMode;

    @Override
    public String getDataSourceKey() {
        return dataSourceKey;
    }

    @Override
    public DbMapping getMapping() {
        return dbMapping;
    }

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getOuterAdapterKey() {
        return outerAdapterKey;
    }

    public void setOuterAdapterKey(String outerAdapterKey) {
        this.outerAdapterKey = outerAdapterKey;
    }

    public DbMapping getDbMapping() {
        return dbMapping;
    }

    public void setDbMapping(DbMapping dbMapping) {
        this.dbMapping = dbMapping;
    }

    public boolean isSignMode() {
        return signMode;
    }

    public void setSignMode(boolean signMode) {
        this.signMode = signMode;
    }

    public void validate() {
    }

    public static class DbMapping implements AdapterMapping {
        private String database;

        private String table;

        private String targetDb;

        private String targetTable;

        private String etlCondition;

        private Map<String, String> allMapColumns;

        private boolean mapAll = false;   // 映射所有字段

        private String signKey = "sign"; //sign key 默认为sign

        private Map<String, String> targetColumns;  // 目标表字段映射

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getTargetDb() {
            return targetDb;
        }

        public void setTargetDb(String targetDb) {
            this.targetDb = targetDb;
        }

        public String getTargetTable() {
            return targetTable;
        }

        public void setTargetTable(String targetTable) {
            this.targetTable = targetTable;
        }

        public void setEtlCondition(String etlCondition) {
            this.etlCondition = etlCondition;
        }

        public Map<String, String> getAllMapColumns() {
            return allMapColumns;
        }

        public void setAllMapColumns(Map<String, String> allMapColumns) {
            this.allMapColumns = allMapColumns;
        }

        @Override
        public String getEtlCondition() {
            return null;
        }

        public boolean getMapAll() {
            return mapAll;
        }

        public void setMapAll(boolean mapAll) {
            this.mapAll = mapAll;
        }

        public Map<String, String> getTargetColumns() {
            return targetColumns;
        }

        public void setTargetColumns(Map<String, String> targetColumns) {
            this.targetColumns = targetColumns;
        }

        public String getSignKey() {
            return signKey;
        }

        public void setSignKey(String signKey) {
            this.signKey = signKey;
        }
    }

}
