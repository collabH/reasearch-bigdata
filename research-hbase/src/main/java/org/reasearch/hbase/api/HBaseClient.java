package org.reasearch.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

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
     * DDL
     * 判断表是否存在
     *
     * @param tableName 表名
     * @return 是否存在
     */
    public static boolean isTableExist(String tableName) throws IOException {
        return connection.getAdmin().tableExists(TableName.valueOf(tableName));
    }


    public static void dropTable(String tableName) throws IOException {
        connection.getAdmin().deleteTable(TableName.valueOf(tableName));
    }

    public static void createTableName(String tableName) throws IOException {
        ColumnFamilyDescriptor student = ColumnFamilyDescriptorBuilder.newBuilder(ColumnFamilyDescriptorBuilder.of("student"))
                .setMaxVersions(1000)
                .build();
        TableDescriptor test = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                .setColumnFamily(student)
                .build();
        connection.getAdmin().createTable(test);
    }

    public static void createNameSpace(String ns) throws IOException {
        connection.getAdmin().createNamespace(NamespaceDescriptor.create(ns).build());
    }


    /**
     * DML
     */
    public static void put(String tableName, String rowKey, String cf, String cn, String value, long ts) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey), ts)
                .addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(put);
    }

    /**
     * get
     */
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey))
                .addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(isTableExist("stu"));
    }
}
