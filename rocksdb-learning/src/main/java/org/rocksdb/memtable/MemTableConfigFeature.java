package org.rocksdb.memtable;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.MemTableConfig;
import org.rocksdb.Options;
import org.rocksdb.VectorMemTableConfig;

/**
 * @fileName: MemTableConfig.java
 * @description: MemTableConfig.java类说明
 * @author: huangshimin
 * @date: 2021/10/13 8:04 下午
 */
public class MemTableConfigFeature {
    public static void main(String[] args) {
        Options options = new Options();
        // 指定memtable底层实现，默认为skiplist，支持hashSkipList，hashlinked，vector等
        options.setMemTableConfig(new VectorMemTableConfig());
        // 单个memtable大小 默认大小64MB
//        options.setWriteBufferSize()
        // 最多的memtable个数默认为2，超过会刷新到sst file中
        options.setMaxWriteBufferNumber(2);
        // 保留的历史的memtable
        options.setMaxWriteBufferNumberToMaintain(0);

    }
}
