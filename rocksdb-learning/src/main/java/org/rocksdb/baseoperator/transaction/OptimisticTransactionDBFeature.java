package org.rocksdb.baseoperator.transaction;

import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.OptimisticTransactionOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.nio.charset.Charset;

/**
 * @fileName: OptimisticTransactionDBFeature.java
 * @description: 乐观锁事务DB
 * @author: huangshimin
 * @date: 2021/9/30 10:33 上午
 */
public class OptimisticTransactionDBFeature {
    private static final String dbPath = "/Users/huangshimin/Documents/study/rocksdb/optimistictransactiondb";

    public static void main(String[] args) {
        Options options = new Options();
        options.setCreateIfMissing(true);
        options.allow2pc();
        options.allowIngestBehind();
        options.allowMmapReads();
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.ignoreMissingColumnFamilies();
        OptimisticTransactionDB.loadLibrary();
        try (OptimisticTransactionDB db = OptimisticTransactionDB.open(options, dbPath)) {
            OptimisticTransactionOptions optimisticTransactionOptions = new OptimisticTransactionOptions();
            Transaction transaction = db.beginTransaction(writeOptions, optimisticTransactionOptions);
            transaction.put("name".getBytes(), "hsm".getBytes());
            System.out.println(new String(transaction.get(new ReadOptions(), "name".getBytes()),
                    Charset.defaultCharset()));
            transaction.commit();
            transaction.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
