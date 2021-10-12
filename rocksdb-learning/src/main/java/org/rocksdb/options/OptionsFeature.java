package org.rocksdb.options;


import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.List;

/**
 * @fileName: OptionsFeature.java
 * @description: options配置
 * @author: huangshimin
 * @date: 2021/9/30 3:56 下午
 */
public class OptionsFeature {

    public static void main(String[] args) throws RocksDBException {
        Options options = new Options();
        // 多个列族原子性flush
        options.setAtomicFlush(true);
        // 设置比较器
        options.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);

        RocksDB db = RocksDB.open("test");
        FlushOptions flushOptions = new FlushOptions();
        db.flush(flushOptions);
    }

    private void tsOpt() throws RocksDBException {
        WriteOptions writeOptions = new WriteOptions();
        ReadOptions readOptions = new ReadOptions();
        RocksDB db = RocksDB.open("test");
        db.get(readOptions, "test".getBytes());
    }


    private void writeBufferSizeOptions() throws Exception {
        DBOptions dbOptions = new DBOptions();
        // 默认大小是关闭的
        dbOptions.setDbWriteBufferSize(64 << 30);
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        // 默认为64MB
        columnFamilyOptions.setWriteBufferSize(64 << 20);
        ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor("name".getBytes(),
                columnFamilyOptions);
        columnFamilyDescriptors.add(columnFamilyDescriptor);
        List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
        RocksDB.open(dbOptions, "test", columnFamilyDescriptors, columnFamilyHandles);
    }

    private void blockSizeOptions() throws Exception {
        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
        // 设置128MB
        blockBasedTableConfig.setBlockCache(new Cache(128 << 20) {
            @Override
            protected void disposeInternal(long handle) {

            }
        });

    }

}
