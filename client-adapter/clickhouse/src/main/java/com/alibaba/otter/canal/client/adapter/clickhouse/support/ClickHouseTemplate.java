package com.alibaba.otter.canal.client.adapter.clickhouse.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by mew on 2021/5/7
 * clickhouse操作类
 **/
public class ClickHouseTemplate {

    private static Logger logger = LoggerFactory.getLogger(ClickHouseTemplate.class);

    private DataSource dataSource;

    public ClickHouseTemplate(DataSource dataSource) {
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

}
