package org.rocksdb.sst;

import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.rocksdb.SstFileWriter;

/**
 * @fileName: SstWriterFeature.java
 * @description: sstWriter写入sst file
 * @author: huangshimin
 * @date: 2021/9/29 3:00 下午
 */
public class SstWriterFeature {
    public static final String path = "/Users/huangshimin/Documents/study/rocksdb/sst/test.sst";
    public static final String dbPath = "/Users/huangshimin/Documents/study/rocksdb/db";

    public static void main(String[] args) {
        EnvOptions envOptions = new EnvOptions();
        envOptions.allowFallocate();
        // 配置ratelimiter
//        envOptions.setRateLimiter();
        Options options = new Options();
        options.allow2pc();
        options.allowConcurrentMemtableWrite();
        try (SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options)) {
            sstFileWriter.open(path);
            sstFileWriter.put(new Slice("test"), new Slice("zhangsan"));
            sstFileWriter.finish();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
