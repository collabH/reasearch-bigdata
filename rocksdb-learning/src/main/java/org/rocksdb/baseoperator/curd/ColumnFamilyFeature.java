package org.rocksdb.baseoperator.curd;

import com.google.common.collect.Lists;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ChecksumType;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.Filter;
import org.rocksdb.HdfsEnv;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.PersistentCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.rocksdb.utils.RocksDBValueParser;

import java.util.List;

/**
 * @fileName: ColumnFamilyFeature.java
 * @description: 列族特性
 * @author: huangshimin
 * @date: 2021/10/20 5:35 下午
 */
public class ColumnFamilyFeature {
    private static String path = "/Users/huangshimin/Documents/study/rocksdb/db";
    private static RocksDBValueParser parser = new RocksDBValueParser();

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
        blockBasedTableConfig.setChecksumType(ChecksumType.kCRC32c);
        // 检索block时判断key是否存在
        blockBasedTableConfig.setFilterPolicy(new BloomFilter());
        // 开启索引压缩
        blockBasedTableConfig.setEnableIndexCompression(true);
        /**
         * 1.kHashSearch:（如果启用）将在提供Options.prefix_extractor时进行哈希查找
         * 2.kTwoLevelIndexSearch。 两个级别都是二分搜索索引。
         * 3.kBinarySearch
         * 4.kBinarySearchWithFirstKey，像kBinarySearch ，但 index 也包含每个块的第一个键。 这允许迭代器推迟读取块，直到真正需要它。
         * 可能会显着减少短程扫描的读取放大。 没有它，迭代器搜索通常从每个 0 级文件和每个级别读取一个块，这可能很昂贵。 结合使用效果最佳： - IndexShorteningMode::kNoShortening， - 自定义 FlushBlockPolicy 在某些有意义的边界处切割块，例如当前缀更改时。 使索引明显更大（2 倍或更多），尤其是当键很长时
         */
        blockBasedTableConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
        // 对每个 SST 文件使用分区的完整过滤器。 此选项与基于block不兼容。 默认为flase。
        blockBasedTableConfig.setPartitionFilters(true);
//        blockBasedTableConfig.setPersistentCache(new PersistentCache(new HdfsEnv("test"),path,100))
        blockBasedTableConfig.setPinTopLevelIndexAndFilter(true);
        // 示我们是否将索引/过滤器块放入块缓存。
        blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
        // index和filter是否高优先级
        blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
        // 指示我们是否要将 L0 索引/过滤器块固定到块缓存。 如果未指定，则默认为 false。
        blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
        columnFamilyOptions.setTableFormatConfig(blockBasedTableConfig);
        ColumnFamilyDescriptor defaultCf = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);
        ColumnFamilyDescriptor userCf = new ColumnFamilyDescriptor("user".getBytes(), columnFamilyOptions);
        columnFamilyDescriptors.add(defaultCf);
        columnFamilyDescriptors.add(userCf);
        RocksDB.loadLibrary();
        DBOptions dbOptions = new DBOptions();
        // 后台flush和compaction的线程数
        dbOptions.setIncreaseParallelism(4);
        // 使用列族需要先创建列族
        try (final RocksDB db = RocksDB.open(dbOptions, path, columnFamilyDescriptors, columnFamilyHandles)) {
//            db.createColumnFamilies(columnFamilyDescriptors);
            WriteOptions writeOptions = new WriteOptions();
            ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(1);
//            db.put(columnFamilyHandle, writeOptions, "name".getBytes(), "hsm".getBytes());
//            db.put(columnFamilyHandle, writeOptions, "age".getBytes(), "11".getBytes());
            RocksIterator rocksIterator = db.newIterator(columnFamilyHandle);
            rocksIterator.seekToFirst();
            // 读取特定列族数据
            while (rocksIterator.isValid()) {
                System.out.println(parser.apply(rocksIterator.key()) + ":" + parser.apply(rocksIterator.value()));
                rocksIterator.next();
            }
            // get
            System.out.println(parser.apply(db.get(columnFamilyHandle, "name".getBytes())));
            System.out.println(parser.apply(db.get(columnFamilyHandles.get(0), "name".getBytes())));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}