package com.research.training;

import com.google.common.collect.Maps;
import com.mysql.jdbc.Driver;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

/**
 * @fileName: MySqlSource.java
 * @description: MySqlSource.java类说明
 * @author: by echo huang
 * @date: 2020-03-01 16:10
 */
public class MySqlSource extends RichParallelSourceFunction<Map<String, String>> {
    private Connection connection;
    private PreparedStatement ps;

    @Override
    public void run(SourceContext<Map<String, String>> ctx) throws Exception {
        Map<String, String> data = Maps.newHashMap();
        ResultSet resultSet = ps.getResultSet();
        while (resultSet.next()) {
            data.put(resultSet.getString("domain"), resultSet.getString("user_id"));
        }
        ctx.collect(data);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DriverManager.getConnection("jdbc:mysql:///sandbox-monitor", "root", "root");
        ps = connection.prepareStatement("select  user_id,domain from user_domain_config");
        ps.execute();
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void cancel() {

    }
}
