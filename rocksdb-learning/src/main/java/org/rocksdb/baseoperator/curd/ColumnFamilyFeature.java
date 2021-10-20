package org.rocksdb.baseoperator.curd;

import com.google.common.collect.Lists;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.nio.charset.Charset;
import java.util.List;

/**
 * @fileName: ColumnFamilyFeature.java
 * @description: 列族特性
 * @author: huangshimin
 * @date: 2021/10/20 5:35 下午
 */
public class ColumnFamilyFeature {
    private static String path = "/Users/huangshimin/Documents/study/rocksdb/db";

    public static void main(String[] args) {
        List<ColumnFamilyHandle> columnFamilyHandles = Lists.newArrayList();
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = Lists.newArrayList();
        // 列族配置
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        columnFamilyOptions.setCompactionStyle(CompactionStyle.LEVEL);
        // 动态分配每一level的compaction bytes大小
        columnFamilyOptions.setLevelCompactionDynamicLevelBytes(true);
        columnFamilyOptions.setWriteBufferSize(64 << 20);
        columnFamilyOptions.setMaxWriteBufferNumber(2);
        // table config
        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
        // 每个数据block的大小
        blockBasedTableConfig.setBlockSize(4 << 10);
        // 每个元数据块的大小当开启index或者filter的时候
        blockBasedTableConfig.setMetadataBlockSize(4 << 10);
        // 设置cache过期策略
        blockBasedTableConfig.setBlockCache(new LRUCache(100));
        columnFamilyOptions.setTableFormatConfig(blockBasedTableConfig);
        ColumnFamilyDescriptor defaultCf = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        ColumnFamilyDescriptor userCf = new ColumnFamilyDescriptor("user".getBytes(), columnFamilyOptions);
        columnFamilyDescriptors.add(defaultCf);
        columnFamilyDescriptors.add(userCf);
        RocksDB.loadLibrary();
        DBOptions dbOptions = new DBOptions();
        // 后台flush和compaction的线程数
        dbOptions.setIncreaseParallelism(4);
        try (final RocksDB db = RocksDB.open(dbOptions, path, columnFamilyDescriptors, columnFamilyHandles)) {
            db.createColumnFamilies(columnFamilyDescriptors);
            WriteOptions writeOptions = new WriteOptions();
            db.put(writeOptions, "name".getBytes(), "hsm".getBytes());
            db.put(writeOptions, "age".getBytes(), "25".getBytes());
            for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                System.out.println(new String(columnFamilyHandle.getName(), Charset.defaultCharset()));
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}