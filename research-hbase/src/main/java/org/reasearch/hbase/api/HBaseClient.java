package org.reasearch.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.security.User;

import java.io.IOException;

/**
 * @fileName: HBaseClient.java
 * @description: HbaseAPI使用
 * @author: by echo huang
 * @date: 2020-08-05 21:50
 */
public class HBaseClient {
    private static Connection connection;

    static {
        connection = getConnection();
    }

    private static Connection getConnection() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop");
        configuration.set("hbase.zookeeper.property.clientPort", "2181"); //your port
        configuration.set("hbase.master", "hadoop:16000");
        try (Connection connection = ConnectionFactory.createConnection(configuration, User.getCurrent())) {
            return connection;
        } catch (IOException e) {
            e.printStackTrace();
            throw new NullPointerException();
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName 表名
     * @return 是否存在
     */
    public static boolean isTableExist(String tableName) throws IOException {
        return connection.getAdmin().tableExists(TableName.valueOf(tableName));
    }


    public static void createTableName(String tableName) throws IOException {
        TableDescriptor test = TableDescriptorBuilder.newBuilder(TableName.valueOf("test")).build();
        connection.getAdmin().createTable(test);
    }

    public static void main(String[] args) throws Exception {
        System.out.println(isTableExist("stu"));
    }
}
