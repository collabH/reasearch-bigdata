package org.rocksdb.db;

import com.google.common.collect.Lists;
import org.rocksdb.Options;
import org.rocksdb.Range;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.function.Consumer;

/**
 * @fileName: SecondaryDatabase.java
 * @description: SecondaryDatabase.java类说明
 * @author: huangshimin
 * @date: 2021/10/3 9:10 下午
 */
public class SecondaryDatabase {
    public static void main(String[] args) {
        Options options = new Options();
        options.setMaxOpenFiles(-1);
        String secondaryPath = "/Users/xiamuguizhi/Documents/reserch/middleware/rocksdb/secondary/";
        String path = "/Users/xiamuguizhi/Documents/reserch/middleware/rocksdb/";
        try {
            RocksDB rocksDB = RocksDB.open(path);
//            RocksDB rocksDB = RocksDB.openAsSecondary(options, path, secondaryPath);
//            rocksDB.tryCatchUpWithPrimary();
            rocksDB.put("name".getBytes(), "hsm".getBytes());
            System.out.println(new String(rocksDB.get("name".getBytes()), Charset.defaultCharset()));
            Range range = new Range(new Slice("name"), new Slice("name"));
            long[] approximateSizes = rocksDB.getApproximateSizes(Lists.newArrayList(range));
            Arrays.stream(approximateSizes).iterator().forEachRemaining((Consumer<Long>) System.out::println);
            RocksDB.CountAndSize approximateMemTableStats = rocksDB.getApproximateMemTableStats(range);
            System.out.println(approximateMemTableStats.size + ":" + approximateMemTableStats.count);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
