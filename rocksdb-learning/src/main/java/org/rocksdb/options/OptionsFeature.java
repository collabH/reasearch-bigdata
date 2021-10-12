package org.rocksdb.options;


import org.rocksdb.BuiltinComparator;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
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
        // 设置比较器
        options.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);

        RocksDB db = RocksDB.open("test");
        FlushOptions flushOptions = new FlushOptions();
        db.flush(flushOptions);
    }

    private void tsOpt() throws RocksDBException {
        WriteOptions writeOptions = new WriteOptions();
        ReadOptions readOptions = new ReadOptions();
        RocksDB db = RocksDB.open("test");
        db.get(readOptions, "test".getBytes());
    }
}
