package com.alibaba.otter.canal.client.adapter.clickhouse.support;

import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;

/**
 * Created by jiangtiteng on 2021/5/13
 */
public class BatchExecutor {
    public static int doBatchUpdate(Dml dml, MappingConfig mappingConfig, Connection connection) {
        return 0;
    }

    public static int doBatchDelete(Dml dml, MappingConfig mappingConfig, Connection connection) {
        return 0;
    }

    public static int doBatchInsert(Dml dml, MappingConfig mappingConfig, Connection connection) {
        return 0;
    }

    public static int doBatchInsertInSignMode(List<Dml> dmls, MappingConfig mappingConfig, Connection connection) {
        return 0;
    }

}
