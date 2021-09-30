package org.rocksdb.sst;

import com.google.common.collect.Lists;
import org.rocksdb.DBOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.Charset;
import java.util.Arrays;

import static org.rocksdb.sst.SstWriterFeature.dbPath;
import static org.rocksdb.sst.SstWriterFeature.path;

/**
 * @fileName: SstIngestFeature.java
 * @description: SstIngestFeature.java类说明
 * @author: huangshimin
 * @date: 2021/9/29 5:23 下午
 */
public class SstIngestFeature {
    public static void main(String[] args) {
        RocksDB.loadLibrary();
        IngestExternalFileOptions ingestExternalFileOptions = new IngestExternalFileOptions();
        // 跳过重复key覆盖
        ingestExternalFileOptions.setIngestBehind(true);
        try (RocksDB db = RocksDB.open(dbPath)){
            db.ingestExternalFile(Lists.newArrayList(path),ingestExternalFileOptions);
            System.out.println(new String(db.get("test".getBytes()), Charset.defaultCharset()));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
