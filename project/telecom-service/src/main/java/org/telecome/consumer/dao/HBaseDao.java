package org.telecome.consumer.dao;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.telecome.common.domain.BaseDao;

/**
 * @fileName: HBaseDao.java
 * @description: HBaseDao.java类说明
 * @author: by echo huang
 * @date: 2020-08-11 23:21
 */
public class HBaseDao extends BaseDao {
    /**
     * 初始化
     */
    public void init() throws Exception {
        start();
        // 创建命名空间和表
        createNameSpace("ct");
        createTable("ct:teamcom", 6, Lists.newArrayList("caller"));
        stop();
    }

    /**
     * 插入数据
     *
     * @param value
     */
    public void insertData(String value) throws Exception {
        String[] values = value.split("\t");
        if (values.length != 4) {
            throw new IllegalArgumentException("callLog异常");
        }
        String call1 = values[0];
        String call2 = values[1];
        String calltime = values[2];
        String duration = values[3];

        int regionNum = genRegionNum(call1, calltime);

        String rowKey = regionNum + "_" + call1 + "_" + calltime + "_" + call2 + "_" + duration;

        Put put = new Put(Bytes.toBytes(rowKey));
        byte[] cf = Bytes.toBytes("caller");
        put.addColumn(cf, Bytes.toBytes("call1"), Bytes.toBytes(call1));
        put.addColumn(cf, Bytes.toBytes("call2"), Bytes.toBytes(call2));
        put.addColumn(cf, Bytes.toBytes("calltime"), Bytes.toBytes(calltime));
        put.addColumn(cf, Bytes.toBytes("duration"), Bytes.toBytes(duration));
        putData("ct:teamcom", put);
    }
}
