package com.spark.utils;

import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

/**
 * @fileName: MysqlConnectionPool.java
 * @description: MysqlConnectionPool.java类说明
 * @author: by echo huang
 * @date: 2020-04-25 21:47
 */
public class MysqlConnectionPool {

    private static final Map<String, Connection> CONNECTION_POOL = Maps.newConcurrentMap();

    private static Connection apply(String k) {
        try {
            return DriverManager.getConnection("jdbc:mysql://localhost:3306/ds0", "root", "root");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Connection getConnection() throws SQLException {
        return CONNECTION_POOL.computeIfAbsent("a", MysqlConnectionPool::apply);
    }
}
