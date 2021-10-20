package org.rocksdb.baseoperator.curd;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.StringAppendOperator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.charset.Charset;

/**
 * @fileName: BatchWriteFeature.java
 * @description: 批量写入
 * @author: huangshimin
 * @date: 2021/10/20 2:52 下午
 */
public class BatchWriteFeature {
    private static String path = "/Users/huangshimin/Documents/study/rocksdb/db";

    public static void main(String[] args) {
        RocksDB.loadLibrary();
        Options options = new Options();
        try (final RocksDB db = RocksDB.open(options, path)) {
            WriteOptions writeOptions = new WriteOptions();
            WriteBatch writeBatch = new WriteBatch();
            writeBatch.put("name1".getBytes(),"h".getBytes());
            writeBatch.put("name2".getBytes(),"h1".getBytes());
            db.write(writeOptions,writeBatch);
            System.out.println(new String(db.get("name1".getBytes()), Charset.defaultCharset()));
            System.out.println(new String(db.get("name2".getBytes()), Charset.defaultCharset()));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
