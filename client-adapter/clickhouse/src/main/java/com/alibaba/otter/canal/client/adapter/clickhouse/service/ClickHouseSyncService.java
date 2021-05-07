package com.alibaba.otter.canal.client.adapter.clickhouse.service;

import com.alibaba.otter.canal.client.adapter.clickhouse.config.MappingConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by jiangtiteng on 2021/5/7
 */
public class ClickHouseSyncService {
    private DataSource dataSource;

    private Map<String, Map<String, MappingConfig>> mappingConfigCache;

    private Boolean isTcpMode;

    public ClickHouseSyncService(DataSource dataSource,
                                 Map<String, Map<String, MappingConfig>> mappingConfigCache,
                                 Boolean isTcpMode
                                 ) {
        this.dataSource = dataSource;
        this.mappingConfigCache = mappingConfigCache;
        this.isTcpMode = isTcpMode;
    }

    public void sync(List<Dml> dmls) {

    }


}
