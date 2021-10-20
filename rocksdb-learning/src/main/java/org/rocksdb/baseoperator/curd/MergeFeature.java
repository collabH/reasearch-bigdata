package org.rocksdb.baseoperator.curd;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteOptions;

import java.nio.charset.Charset;

/**
 * @fileName: MergeFeature.java
 * @description: MergeFeature.java类说明
 * @author: huangshimin
 * @date: 2021/10/20 2:06 下午
 */
public class MergeFeature {
    private static String path = "/Users/huangshimin/Documents/study/rocksdb/db";

    public static void main(String[] args) {
        RocksDB.loadLibrary();
        Options options = new Options();
        options.setMergeOperator(new StringAppendOperator(':'));
        try (final RocksDB db = RocksDB.open(options, path)) {
            WriteOptions writeOptions = new WriteOptions();
            writeOptions.setIgnoreMissingColumnFamilies(true);
            db.merge(writeOptions, "name".getBytes(), "hsm".getBytes());
            System.out.println(new String(db.get("name".getBytes()), Charset.defaultCharset()));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
