package org.reasearch.hbase.demo.infrastructure.utils;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.reasearch.hbase.demo.infrastructure.constants.HBaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @fileName: HBaseUtils.java
 * @description: HBaseUtils.java类说明
 * @author: by echo huang
 * @date: 2020-08-08 23:26
 */
public class HBaseUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseUtils.class);

    /**
     * 创建命名空间
     *
     * @param nameSpace
     */
    public static void createNameSpace(String nameSpace) throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(nameSpace).
                build();
        admin.createNamespace(descriptor);

        admin.close();
        connection.close();
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param version
     * @param columnFamilies
     */
    public static void createTable(String tableName, int version, String... columnFamilies) throws IOException {
        if (ArrayUtils.isEmpty(columnFamilies)) {
            LOGGER.error("列族配置不能为null");
            return;
        }
        boolean tableExist = isTableExist(tableName);
        if (tableExist) {
            LOGGER.error("该表已经存在");
            return;
        }
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();

        TableDescriptorBuilder descriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

        List<ColumnFamilyDescriptor> cfs = Lists.newArrayList();
        for (String columnFamily : columnFamilies) {
            cfs.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily))
                    .setMaxVersions(version).build());
        }

        descriptorBuilder.setColumnFamilies(cfs);

        admin.createTable(descriptorBuilder.build());


        admin.close();
        connection.close();
    }


    /**
     * 获取连接
     *
     * @return
     * @throws IOException
     */
    public static Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(HBaseConfig.configuration);
    }

    /**
     * 判断表是否存在
     *
     * @return
     */
    private static boolean isTableExist(String tableName) throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        admin.close();
        connection.close();
        return exists;
    }
}
