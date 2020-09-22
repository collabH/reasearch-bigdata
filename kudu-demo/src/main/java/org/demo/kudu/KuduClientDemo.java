package org.demo.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.com.google.common.collect.Lists;

import java.util.List;

/**
 * @fileName: KuduClient.java
 * @description: KuduClient.java类说明
 * @author: by echo huang
 * @date: 2020/9/22 11:05 上午
 */
public class KuduClientDemo {

    private static final String KUDU_MASTER_SERVER_ADDRESS = "192.168.6.50:7051,192.168.6.52:7051,192.168.6.51:7051";

    private static final String KUDU_TABLE_NAME = "test_kud";

    public static void main(String[] args) {
        // 创建KuduClient
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER_SERVER_ADDRESS)
                .build();

        // 添加列
        List<ColumnSchema> columns = Lists.newArrayList();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build());

        // 创建schema
        Schema schema = new Schema(columns);
        // 定义范围分区列表
        List<String> rangeKeys = Lists.newArrayList();
        rangeKeys.add("key");

        try {
            // 创建表
//            createKudu(client, schema, rangeKeys);
            // 插入数据
//            insertKudu(client);
            // 查询数据
            queryKudu(client);
            client.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    private static void queryKudu(KuduClient client) throws KuduException {

        List<String> projectColumns = Lists.newArrayList();
        projectColumns.add("value");

        // 构建扫描器
        KuduScanner scanner = client.newScannerBuilder(client.openTable(KUDU_TABLE_NAME))
                .setProjectedColumnNames(projectColumns)
                .build();
        while (scanner.hasMoreRows()) {
            RowResultIterator rowResults = scanner.nextRows();
            while (rowResults.hasNext()){
                RowResult next = rowResults.next();
                System.out.println(next);
            }
        }

    }


    private static void createKudu(KuduClient client, Schema schema, List<String> rangeKeys) throws KuduException {
        // 创建表
        KuduTable createTable = client.createTable(KUDU_TABLE_NAME, schema, new CreateTableOptions()
                .setRangePartitionColumns(rangeKeys)
                .setOwner("huangshimin")
                .setNumReplicas(1));
        System.out.println(createTable.toString());
    }

    private static void insertKudu(KuduClient client) throws KuduException {
        // 插入数据
        KuduTable kuduTable = client.openTable(KUDU_TABLE_NAME);
        KuduSession session = client.newSession();
        // 获取insert对象
        Insert insert = kuduTable.newInsert();
        PartialRow row = insert.getRow();
        row.addInt(0, 1);
        row.addString(1, "sbhejiawang");
        // 执行插入数据
        session.apply(insert);
    }
}
