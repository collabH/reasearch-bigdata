package org.rocksdb.demo;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @fileName: RocksDbTest.java
 * @description: RocksDbTest.java类说明
 * @author: by echo huang
 * @date: 2021/1/26 11:33 上午
 */
public class RocksDbTest {
    private static final String DB_PATH = "/Users/babywang/Desktop/testrocksdb";

    private static final String HDFS_DB_PATH = "hdfs://cluster01/user/flink";

    static {
        RocksDB.loadLibrary();
    }
    public static void main(String[] args) {
//        Env env = new HdfsEnv();
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (final RocksDB db = RocksDB.open(options, HDFS_DB_PATH)) {
                db.put("test".getBytes(), "张三".getBytes(StandardCharsets.UTF_8));
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }
    }

    public void openDataBaseWithColumnFamilies() {
        RocksDB.loadLibrary();
        try (final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions().optimizeUniversalStyleCompaction()) {
            final List<ColumnFamilyDescriptor> cfDes = Arrays.asList(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                    new ColumnFamilyDescriptor("my-first-columnfamily".getBytes(), columnFamilyOptions));
            final List<ColumnFamilyHandle> columnFamilyHandleList =
                    new ArrayList<>();
            try (final DBOptions options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
                 final RocksDB db = RocksDB.open(options,
                         DB_PATH, cfDes,
                         columnFamilyHandleList)) {
                try {

                    db.write(new WriteOptions().setSync(true),new WriteBatch());
                    // do something

                } finally {

                    // NOTE frees the column family handles before freeing the db
                    for (final ColumnFamilyHandle columnFamilyHandle :
                            columnFamilyHandleList) {
                        columnFamilyHandle.close();
                    }
                } // frees the db and the db options
            } catch (RocksDBException e) {
                e.printStackTrace();
            }
        }
    }

    public void openDataBase() {
        // 一个静态方法加载RocksDB C++ lib
        RocksDB.loadLibrary();
        try (final Options options = new Options().setCreateIfMissing(true)) {
            try (RocksDB open = RocksDB.open(options, DB_PATH)) {
//                open.put("a".getBytes(), "hehe".getBytes(StandardCharsets.UTF_8));
//                open.flush(new FlushOptions().setWaitForFlush(true));
                System.out.println(new String(open.get("a".getBytes()), StandardCharsets.UTF_8));
                open.delete("a".getBytes());
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
