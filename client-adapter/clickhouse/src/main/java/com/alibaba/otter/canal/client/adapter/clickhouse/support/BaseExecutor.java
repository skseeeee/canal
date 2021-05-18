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
public class BaseExecutor {

    private static Logger logger = LoggerFactory.getLogger(BaseExecutor.class);

    protected DataSource dataSource;

    public BaseExecutor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    protected Connection conn;

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

    public void close() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

}
