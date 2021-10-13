package org.rocksdb.options;

import com.google.common.collect.Lists;
import org.rocksdb.DBOptions;
import org.rocksdb.HdfsEnv;
import org.rocksdb.RocksDB;
import org.rocksdb.SstFileManager;

/**
 * @fileName: FileManagerFeature.java
 * @description: FileManagerFeature.java类说明
 * @author: huangshimin
 * @date: 2021/10/13 7:44 下午
 */
public class FileManagerFeature {
    public static void main(String[] args) throws Exception {
        DBOptions dbOptions = new DBOptions();
        HdfsEnv hdfs = new HdfsEnv("hdfs://hostname:port/");
        hdfs.setBackgroundThreads(10);
        // 存储sstFile文件
        dbOptions.setSstFileManager(new SstFileManager(hdfs));
        RocksDB hdfsDB = RocksDB.open(dbOptions, "test", Lists.newArrayList(),
                Lists.newArrayList());
        hdfsDB.put("name".getBytes(), "hsm".getBytes());
    }
}
