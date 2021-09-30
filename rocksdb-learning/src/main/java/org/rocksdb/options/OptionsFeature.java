package org.rocksdb.options;


import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

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

        RocksDB db = RocksDB.open("test");
        FlushOptions flushOptions = new FlushOptions();
        db.flush(flushOptions);
    }
}
