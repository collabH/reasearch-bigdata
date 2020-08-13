package org.telecome.consumer.dao;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * @fileName: HBaseCoProcess.java
 * @description: 协处理器
 * @author: by echo huang
 * @date: 2020-08-13 22:15
 */
public class HBaseCoProcess extends BaseRegionObserver {
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

        Table table = e.getEnvironment().getTable(TableName.valueOf("ct:telecom"));

        // 在put之后 hbase做的操作
    }
}
