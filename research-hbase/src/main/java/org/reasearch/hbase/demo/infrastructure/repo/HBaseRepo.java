package org.reasearch.hbase.demo.infrastructure.repo;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.reasearch.hbase.demo.infrastructure.constants.HBaseConfig;
import org.reasearch.hbase.demo.infrastructure.utils.HBaseUtils;

import java.io.IOException;
import java.util.List;

/**
 * @fileName: HBaseRepo.java
 * @description: HBaseRepo.java类说明
 * @author: by echo huang
 * @date: 2020-08-09 11:29
 */
public class HBaseRepo {


    /**
     * 发布消息
     */
    public void pulish(String uid, String content) throws IOException {
        Connection connection = HBaseUtils.getConnection();
        // content表
        Table contentTable = connection.getTable(TableName.valueOf(HBaseConfig.contentTable));

        long ts = System.currentTimeMillis();


        // rowKey规则
        String rowKey = uid + "_" + ts;


        Put put = new Put(Bytes.toBytes(rowKey));

        put.addColumn(Bytes.toBytes(HBaseConfig.contentTableCf), Bytes.toBytes("content"), Bytes.toBytes(content));


        contentTable.put(put);

        //relation 表

        Table relationTable = connection.getTable(TableName.valueOf(HBaseConfig.relationTable));

        Get get = new Get(Bytes.toBytes(uid));
        // 查询全部粉丝
        get.addFamily(Bytes.toBytes(HBaseConfig.relationTableCf2));
        Result result = relationTable.get(get);


        List<Put> inboxList = Lists.newArrayList();
        // 存储微博内容表对象
        for (Cell cell : result.rawCells()) {
            //拿到全部的粉丝的列构造inbox的put对象
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            // 封装列族、列、value数据
            inboxPut.addColumn(Bytes.toBytes(HBaseConfig.inboxTableCF),
                    Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            inboxList.add(inboxPut);
        }

        // 判断是否有粉丝

        if (CollectionUtils.isNotEmpty(inboxList)) {
            //获取收件箱表对象
            Table inboxTable = connection.getTable(TableName.valueOf(HBaseConfig.inboxTable));
            inboxTable.put(inboxList);

            inboxTable.close();
        }

        relationTable.close();
        contentTable.close();
        connection.close();

    }


    /**
     * 关注用户
     */
    public void addAttends(String uid, List<String> attendUids) throws IOException {
        if (CollectionUtils.isEmpty(attendUids)) {
            return;
        }
        Connection connection = HBaseUtils.getConnection();

        // 操作关系表
        Table relationTable = connection.getTable(TableName.valueOf(HBaseConfig.relationTable));

        List<Put> relationPut = Lists.newArrayList();
        Put fanPut = new Put(Bytes.toBytes(uid));


        for (String attendUid : attendUids) {
            // 添加关注对象
            fanPut.addColumn(Bytes.toBytes(HBaseConfig.relationTableCf1), Bytes.toBytes(attendUid), Bytes.toBytes(attendUid));
            Put attendPut = new Put(Bytes.toBytes(attendUid));
            // 添加粉丝
            attendPut.addColumn(Bytes.toBytes(HBaseConfig.relationTableCf2), Bytes.toBytes(uid), Bytes.toBytes(uid));
            relationPut.add(attendPut);

        }

        relationPut.add(fanPut);

        // 操作表
        relationTable.put(relationPut);

        // 拉取微博
        Table inboxTable = connection.getTable(TableName.valueOf(HBaseConfig.inboxTable));

        // 拉取粉丝订阅微博
        Put inboxFansPut = new Put(Bytes.toBytes(uid));


        // 拉取微博内容
        Table contentTable = connection.getTable(TableName.valueOf(HBaseConfig.contentTable));

        // 拉取微博内容
        for (String attendUid : attendUids) {
            // 拉取每个attendUid微博内容
            ResultScanner scanner = contentTable.getScanner(new Scan(Bytes.toBytes(attendUid + "_"), Bytes.toBytes(attendUid + "_" + Long.MAX_VALUE)));
            long ts = System.currentTimeMillis();
            // 拉取内容拼接inboxput
            for (Result result : scanner) {
                inboxFansPut.addColumn(Bytes.toBytes(HBaseConfig.inboxTableCF), Bytes.toBytes(attendUid), ts++,
                        result.getRow());
            }
        }

        if (inboxFansPut.isEmpty()) {
            inboxTable.put(inboxFansPut);
            inboxTable.close();
        }

        relationTable.close();
        contentTable.close();
        connection.close();

    }


    /**
     * 取消关注
     *
     * @param uid
     * @param attendUids
     */
    public void deleteAttends(String uid, List<String> attendUids) throws IOException {

        if (CollectionUtils.isEmpty(attendUids)) {
            return;
        }
        Connection connection = HBaseUtils.getConnection();

        // 操作关系表
        Table relationTable = connection.getTable(TableName.valueOf(HBaseConfig.relationTable));


        Delete attendsDelete = new Delete(Bytes.toBytes(uid));
        List<Delete> fansDeletedList = Lists.newArrayList();

        List<Delete> inboxDeletedList = Lists.newArrayList();

        for (String attendUid : attendUids) {
            //type: DeleteColumn
            attendsDelete.addColumns(Bytes.toBytes(HBaseConfig.relationTableCf1), Bytes.toBytes(attendUid));

            Delete fansDeleted = new Delete(Bytes.toBytes(attendUid));

            fansDeleted.addColumns(Bytes.toBytes(HBaseConfig.relationTableCf2), Bytes.toBytes(uid));
            fansDeletedList.add(fansDeleted);

            Delete inboxDelete = new Delete(Bytes.toBytes(uid));
            inboxDelete.addColumns(Bytes.toBytes(HBaseConfig.inboxTableCF), Bytes.toBytes(attendUid));

            inboxDeletedList.add(inboxDelete);
        }

        fansDeletedList.add(attendsDelete);
        // 删除粉丝
        relationTable.delete(fansDeletedList);


        // inbox表
        Table inboxTable = connection.getTable(TableName.valueOf(HBaseConfig.inboxTable));

        // 删除订阅微博
        inboxTable.delete(inboxDeletedList);


        inboxTable.close();
        relationTable.close();
        connection.close();
    }

    /**
     * 获取数据
     *
     * @param uid
     * @throws IOException
     */
    public void getData(String uid) throws IOException {
        Connection connection = HBaseUtils.getConnection();
        Table inboxTable = connection.getTable(TableName.valueOf(HBaseConfig.inboxTable));
        Get inboxGet = new Get(Bytes.toBytes(uid));
        Result iboxResult = inboxTable.get(inboxGet);

        List<Get> contentGet = Lists.newArrayList();
        for (Cell cell : iboxResult.rawCells()) {
            contentGet.add(new Get(CellUtil.cloneValue(cell)));
        }
        Table contentTable = connection.getTable(TableName.valueOf(HBaseConfig.contentTable));

        Result[] results = contentTable.get(contentGet);

        for (Result result : results) {
            System.out.println("content:" + Bytes.toString(result.getValue(Bytes.toBytes(HBaseConfig.contentTableCf), Bytes.toBytes("content"))));
        }

        inboxTable.close();
        contentTable.close();
        connection.close();

    }

    /**
     * 查询某个人的微博详情
     *
     * @param attendId
     */
    public void getAttendContentList(String attendId) throws IOException {
        Connection connection = HBaseUtils.getConnection();
        Table contentTable = connection.getTable(TableName.valueOf(HBaseConfig.contentTable));
        // 使用过滤器
        Scan scan = new Scan();
//        scan.setRowPrefixFilter(Bytes.toBytes(attendId));

        RowFilter rowFilter = new RowFilter(CompareOperator.EQUAL, new SubstringComparator(attendId + "_"));
        scan.setFilter(rowFilter);
        for (Result result : contentTable.getScanner(scan)) {
            System.out.println(result);
        }

        contentTable.close();
    }

}
