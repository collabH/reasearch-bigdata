package org.rocksdb.options;


import com.google.common.collect.Lists;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.HdfsEnv;
import org.rocksdb.Options;
import org.rocksdb.RateLimiter;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.SstFileManager;
import org.rocksdb.StringAppendOperator;
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
        // 设置merge方法的mergeOpeartor
        options.setMergeOperator(new StringAppendOperator());
        // 是否运行并发写入memtable
        options.setAllowConcurrentMemtableWrite(true);
        // 跨列族的buffer size
        options.setWriteBufferSize(64 << 30);
        SkipListMemTableConfig skipListMemTableConfig = new SkipListMemTableConfig();
        // 设置memtable数据结构为skip list
        options.setMemTableConfig(skipListMemTableConfig);
        // compaction策略
        options.setCompactionPriority(CompactionPriority.OldestLargestSeqFirst);
        // 手动wal文件flush
        options.setManualWalFlush(true);
        // 如果不存在就创建
        options.setCreateIfMissing(true);
        // key的ttl
        options.setTtl(10);
        options.setAllow2pc(true);
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
        // compaction和flush的线程数
        dbOptions.setIncreaseParallelism(4);
        // sst文件放入hdfs
        dbOptions.setSstFileManager(new SstFileManager(new HdfsEnv("hdfspath")));
        // 限流
        dbOptions.setRateLimiter(new RateLimiter(10));
        // 列族如果不存在就创建
        dbOptions.setCreateMissingColumnFamilies(true);
        dbOptions.setAllowMmapReads(true);
        // 设置wal dir
        dbOptions.setWalDir("wal");
        dbOptions.setDbLogDir("");
        // dbpath
        dbOptions.setDbPaths(Lists.newArrayList());
        // 设置执行env
        dbOptions.setEnv(new HdfsEnv("test"));
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        // 默认为64MB
        columnFamilyOptions.setWriteBufferSize(64 << 20);
        // 设置level压缩风格
        columnFamilyOptions.setCompactionStyle(CompactionStyle.LEVEL);
        // 最大的writeBuffer个数
        columnFamilyOptions.setMaxWriteBufferNumber(6);
        // 动态设置每一level的大小
        columnFamilyOptions.setLevelCompactionDynamicLevelBytes(true);
        // 目标文件大小
//        columnFamilyOptions.setTargetFileSizeBase();
        // 设置特定层数的压缩格式
        CompressionOptions compressionOptions = new CompressionOptions();
        columnFamilyOptions.setCompressionOptions(compressionOptions);
        // 设置每一次的压缩算法
        columnFamilyOptions.setCompressionPerLevel(Lists.newArrayList(CompressionType.BZLIB2_COMPRESSION));
        // 设置每个列族的路径
//        columnFamilyOptions.setCfPaths();
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

    private void compression() throws Exception {
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        // 第n-1层压缩格式
        columnFamilyOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
        columnFamilyOptions.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
        CompressionOptions compressionOptions = new CompressionOptions();
        compressionOptions.setEnabled(true);
        compressionOptions.setLevel(1);
        columnFamilyOptions.setCompressionOptions(compressionOptions);
        // 指定前n level的压缩类型
//        columnFamilyOptions.setCompressionPerLevel()

        // 第n层压缩格式
        columnFamilyOptions.setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);
        columnFamilyOptions.setBottommostCompressionType(CompressionType.ZLIB_COMPRESSION);
    }

    private void ratelimiter() throws Exception {
        DBOptions dbOptions = new DBOptions();
        // db每秒200 qps
        dbOptions.setRateLimiter(new RateLimiter(200));
    }

}
