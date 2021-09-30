package org.rocksdb.curd;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static org.rocksdb.sst.SstWriterFeature.dbPath;

/**
 * @fileName: DeleteFeature.java
 * @description: 删除特性
 * @author: huangshimin
 * @date: 2021/9/29 7:44 下午
 */
public class DeleteFeature {
    private static RocksDB db = null;

    static {
        try {
            RocksDB.loadLibrary();
            Options options = new Options();
            options.setTtl(1001);
            db = RocksDB.open(options,dbPath);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

    }

    private static void writeConfig(){
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.disableWAL();
        writeOptions.ignoreMissingColumnFamilies();
        // low Priority
        writeOptions.setLowPri(true);
    }

    /**
     * 它会删除键的最新版本，而旧版本的键是否会恢复则是未定义的。
     * sinlge delete
     * @param key
     */
    private static void singleDelete(String key) {
        try {
            db.singleDelete(key.getBytes());
            // batch中删除
            WriteBatch writeBatch = new WriteBatch();
            writeBatch.singleDelete(key.getBytes());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
