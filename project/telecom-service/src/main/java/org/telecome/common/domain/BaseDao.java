package org.telecome.common.domain;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @fileName: BaseDao.java
 * @description: BaseDao.java类说明
 * @author: by echo huang
 * @date: 2020-08-11 23:20
 */
@Slf4j
public abstract class BaseDao {

    private static final ThreadLocal<Connection> connHolder = new ThreadLocal<>();
    private static final ThreadLocal<Admin> adminHolder = new ThreadLocal<>();

    protected void start() throws Exception {
        getConnection();
        getAdmin();
    }

    /**
     * 创建命名空间
     *
     * @throws IOException
     */
    protected void createNameSpace(String namespace) throws IOException {
        Admin admin = adminHolder.get();
        String[] namespaces = admin.listNamespaces();
        if (Arrays.asList(namespaces).contains(namespace)) {
            log.error("该nameSpace:{} 已经存在", namespace);
            return;
        }
        NamespaceDescriptor ns = NamespaceDescriptor.create(namespace).build();
        admin.createNamespace(ns);
    }


    protected void createTable(String tableName, List<String> columnFamilies) throws IOException {
        Admin admin = adminHolder.get();
        TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            admin.deleteTable(table);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
        for (String columnFamily : columnFamilies) {
            hTableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
        }
        admin.createTable(hTableDescriptor);
    }

    protected void stop() throws Exception {
        Admin admin = adminHolder.get();
        if (Objects.nonNull(admin)) {
            admin.close();
            adminHolder.remove();
        }
        Connection connection = connHolder.get();
        if (Objects.nonNull(connection)) {
            connection.close();
            connHolder.remove();
        }
    }

    /**
     * 获取连接对象
     *
     * @return
     */
    protected Connection getConnection() throws IOException {
        Connection connection = connHolder.get();
        synchronized (connHolder) {
            if (Objects.isNull(connection)) {
                Configuration configuration = HBaseConfiguration.create();
                connection = ConnectionFactory.createConnection(configuration);
                connHolder.set(connection);
            }
        }
        return connection;
    }

    protected Admin getAdmin() throws IOException {
        Admin admin = adminHolder.get();
        synchronized (connHolder) {
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }
}
