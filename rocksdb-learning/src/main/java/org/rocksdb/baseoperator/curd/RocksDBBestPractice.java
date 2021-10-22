package org.rocksdb.baseoperator.curd;

import com.google.common.collect.Lists;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ConcurrentTaskLimiterImpl;
import org.rocksdb.DBOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.utils.RocksDBValueParser;

import java.util.List;

/**
 * @fileName: RocksDBBestPractice.java
 * @description: Rocksdb最佳实践
 * @author: huangshimin
 * @date: 2021/10/21 8:21 下午
 */
public class RocksDBBestPractice {
    private static final String ROCKS_DB_PATH = "/Users/huangshimin/Documents/study/rocksdb/db";
    private static final String ROCKS_WAL_PATH = "/Users/huangshimin/Documents/study/rocksdb/wal";
    private static final String ROCKS_LOG_PATH = "/Users/huangshimin/Documents/study/rocksdb/log";
    private static final String HDFS_SST_PATH = "hdfs://hadoop:8082/rocksdb";
    private static final String HDFS_DATA_PATH = "/rocksdb/data";
    private static final String HDFS_LOG_PATH = "/rocksdb/log";
    private static final String HDFS_WAL_PATH = "/rocksdb/wal";

    private static final RocksDBValueParser ROCKS_DB_VALUE_PARSER = new RocksDBValueParser();

    public static void main(String[] args) throws RocksDBException {
        RocksDB.loadLibrary();

        DBOptions dbOptions = new DBOptions();
        dbOptions.setWalDir(ROCKS_WAL_PATH);
        dbOptions.setDbLogDir(ROCKS_LOG_PATH);
        dbOptions.setCreateMissingColumnFamilies(true);
        dbOptions.setCreateIfMissing(true);
        dbOptions.setAllowConcurrentMemtableWrite(true);
//        dbOptions.setSstFileManager(new SstFileManager(new HdfsEnv(HDFS_SST_PATH)));
//        dbOptions.setEnv(new HdfsEnv("hdfs://hadoop:8082/"));
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = Lists.newArrayList();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        columnFamilyOptions.setMergeOperator(new StringAppendOperator());
        columnFamilyOptions.setLevelCompactionDynamicLevelBytes(true);
        BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
        blockBasedTableConfig.setFilterPolicy(new BloomFilter());
        blockBasedTableConfig.setBlockCache(new LRUCache(2000));
        blockBasedTableConfig.setEnableIndexCompression(true);
        columnFamilyOptions.setTableFormatConfig(blockBasedTableConfig);
        columnFamilyOptions.setCompactionThreadLimiter(new ConcurrentTaskLimiterImpl("compaction-thread", 4));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor("user".getBytes(), columnFamilyOptions));
        List<ColumnFamilyHandle> columnFamilyHandles = Lists.newArrayList();
        RocksDB db = RocksDB.open(dbOptions, ROCKS_DB_PATH, columnFamilyDescriptors, columnFamilyHandles);
        ColumnFamilyHandle userCf = columnFamilyHandles.get(1);
        /**
         * create
         */

        // put
        db.put(userCf, "name".getBytes(), "huangsm".getBytes());
        db.put(userCf, "age".getBytes(), "24".getBytes());
        // merge
        db.merge(userCf, "name".getBytes(), "lisi".getBytes());
        // get
        System.out.println(ROCKS_DB_VALUE_PARSER.apply(db.get(userCf, "name".getBytes())));
        // mutliGet
        List<byte[]> mutliValues = db.multiGetAsList(Lists.newArrayList(userCf, userCf),
                Lists.newArrayList("name".getBytes(),
                        "age".getBytes()));
        for (byte[] mutliValue : mutliValues) {
            System.out.println(ROCKS_DB_VALUE_PARSER.apply(mutliValue));
        }
        // delete
        db.delete(userCf,"name".getBytes());
        System.out.println(ROCKS_DB_VALUE_PARSER.apply(db.get(userCf, "name".getBytes())));
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            columnFamilyHandle.getDescriptor().getOptions().close();
        }
        dbOptions.close();
        db.close();
    }
}
