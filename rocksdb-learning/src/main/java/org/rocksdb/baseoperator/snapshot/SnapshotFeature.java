package org.rocksdb.baseoperator.snapshot;

import org.apache.hadoop.fs.ReadOption;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;

import java.nio.charset.Charset;

/**
 * @fileName: SnapshotFeature.java
 * @description: 快照特性
 * @author: huangshimin
 * @date: 2021/9/30 2:49 下午
 */
public class SnapshotFeature {
    private static String path = "/Users/huangshimin/Documents/study/rocksdb/db";

    public static void main(String[] args) {
        RocksDB.loadLibrary();
        try (final RocksDB db = RocksDB.open(path)) {
            Snapshot snapshot = db.getSnapshot();
            db.put("name".getBytes(), "hsm1111".getBytes());
            ReadOptions readOptions = new ReadOptions();
            readOptions.setSnapshot(snapshot);
            RocksIterator rocksIterator = db.newIterator(readOptions);
            rocksIterator.seekToFirst();
            while (rocksIterator.isValid()) {
                System.out.println(new String(rocksIterator.key(), Charset.defaultCharset()) + ":" +
                        new String(rocksIterator.value(), Charset.defaultCharset()));
                rocksIterator.next();
            }
//            System.out.println(new String(db.get("name".getBytes()), Charset.defaultCharset()));
//            db.put("name".getBytes(), "hsm1".getBytes());
//            System.out.println(new String(db.get("name".getBytes()), Charset.defaultCharset()));
            db.releaseSnapshot(snapshot);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
