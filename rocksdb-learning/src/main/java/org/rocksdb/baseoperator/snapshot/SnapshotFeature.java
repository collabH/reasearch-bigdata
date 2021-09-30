package org.rocksdb.baseoperator.snapshot;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;

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
            db.put("name".getBytes(), "hsm".getBytes());
            db.releaseSnapshot(snapshot);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
