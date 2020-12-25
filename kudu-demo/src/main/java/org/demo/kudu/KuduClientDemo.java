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

    private static final String KUDU_MASTER_SERVER_ADDRESS = "cdh01:7051,cdh02:7051,cdh03:7051";

    private static final String KUDU_TABLE_NAME = "test_kud";

    public static void main(String[] args) {
        // 创建KuduClient
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTER_SERVER_ADDRESS)
                .build();

        // 添加列
        List<ColumnSchema> columns = Lists.newArrayList();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                .key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build());

        // 创建schema
        Schema schema = new Schema(columns);
        // 定义范围分区列表
        List<String> rangeKeys = Lists.newArrayList();
        rangeKeys.add("key");

        try {
            // 创建表
            createKudu(client, schema, rangeKeys);
            // 插入数据
//            insertKudu(client);
            // 查询数据
//            queryKudu(client);
            client.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    private static void queryKudu(KuduClient client) throws KuduException {

        List<String> projectColumns = Lists.newArrayList();
        projectColumns.add("key");

        // 构建扫描器
        KuduScanner scanner = client.newScannerBuilder(client.openTable(KUDU_TABLE_NAME))
                .setProjectedColumnNames(projectColumns)
                .build();
        while (scanner.hasMoreRows()) {
            RowResultIterator rowResults = scanner.nextRows();
            while (rowResults.hasNext()) {
                RowResult next = rowResults.next();
                System.out.println(next);
            }
        }

    }


    private static void createKudu(KuduClient client, Schema schema, List<String> rangeKeys) throws KuduException {
        try {
            client.deleteTable(KUDU_TABLE_NAME);
        } catch (KuduException e) {

        }
        PartialRow partialRow = schema.newPartialRow();
        PartialRow partialRow1 = schema.newPartialRow();
        partialRow.addInt("key", 10);
        partialRow1.addInt("key", 20);
        // 创建表
        KuduTable createTable = client.createTable(KUDU_TABLE_NAME, schema, new CreateTableOptions()
//                .setRangePartitionColumns(rangeKeys)
                .addRangePartition(partialRow, partialRow1)
                .addHashPartitions(Lists.newArrayList("key"), 4)
                .setOwner("huangshimin")
                .setNumReplicas(1));
        System.out.println(createTable.toString());
    }

    private static void rangePartition(KuduClient client) throws KuduException {
        KuduTable kuduTable = client.openTable(KUDU_TABLE_NAME);

        PartialRow lower = kuduTable.getSchema().newPartialRow();
        lower.addInt("key", 1);

        PartialRow upper = kuduTable.getSchema().newPartialRow();
        upper.addInt("key", 10);

        new CreateTableOptions().addRangePartition(lower, upper);
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

    private static void deleteKudu(KuduClient client) throws KuduException {
        KuduTable kuduTable = client.openTable(KUDU_TABLE_NAME);
        KuduSession kuduSession = client.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        Delete delete = kuduTable.newDelete();
        PartialRow row = delete.getRow();
        row.addInt("key", 1);

        kuduSession.apply(delete);
    }

    private static void updateKudu(KuduClient client) throws KuduException {
        KuduTable kuduTable = client.openTable(KUDU_TABLE_NAME);
        KuduSession kuduSession = client.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        Update update = kuduTable.newUpdate();

        // 修改数据
        PartialRow row = update.getRow();
        row.addInt("id", 1);
        row.addString("value", "hhhh");

        kuduSession.apply(update);
    }
}
