package com.alibaba.otter.canal.client.adapter.clickhouse.support;

import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Created by jiangtiteng on 2021/5/13
 */
public class ColumnsTypeCache {
    private static ColumnsTypeCache columnsTypeCache = new ColumnsTypeCache();

    public static ColumnsTypeCache getInstance() {
        return columnsTypeCache;
    }

    private DataSource dataSource;

    private ColumnsTypeCache() {}

    /**
     * 获取对应表字段的数据类型
     * @param config
     * @return 字段名->字段类型
     */
    public Map<String, Integer> getColumnsTypeByMappingConfig(MappingConfig config) {
        return null;
    }

    /**
     * 设置数据源，只在初始化时候设置
     * @param dataSource
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
