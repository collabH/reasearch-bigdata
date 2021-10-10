package org.rocksdb.db;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.Charset;

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
            RocksDB rocksDB = RocksDB.openAsSecondary(options, path, secondaryPath);
            rocksDB.tryCatchUpWithPrimary();
            rocksDB.put("name".getBytes(), "hsm".getBytes());
            System.out.println(new String(rocksDB.get("name".getBytes()), Charset.defaultCharset()));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
