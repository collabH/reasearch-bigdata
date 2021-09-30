package org.rocksdb.baseoperator.transaction;

import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.rocksdb.TransactionDB;
import org.rocksdb.TransactionDBOptions;
import org.rocksdb.WriteOptions;

import java.nio.charset.Charset;

/**
 * @fileName: TransactionDbFeature.java
 * @description: 悲观锁方式事务DB
 * @author: huangshimin
 * @date: 2021/9/29 8:47 下午
 */
public class TransactionDbFeature {
    public static final String dbPath = "/Users/huangshimin/Documents/study/rocksdb/transactiondb";

    public static void main(String[] args) throws RocksDBException, InterruptedException {
        Options options = new Options();
        options.setCreateIfMissing(true);
        TransactionDBOptions transactionOptions = new TransactionDBOptions();
        TransactionDB.loadLibrary();
        TransactionDB transactionDB = TransactionDB.open(options, transactionOptions, dbPath);
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.ignoreMissingColumnFamilies();
        ReadOptions readOptions = new ReadOptions();

        // 开启事务
//        Transaction transaction = transactionDB.beginTransaction(writeOptions);
//        // 外部的db put会导致事务失败
//        transaction.setSnapshot();
//        transactionDB.put("name".getBytes(), "wy".getBytes());
//        Thread.sleep(5000);
//        transaction.put("name".getBytes(), "hsm".getBytes());
//        System.out.println(new String(transaction.get(readOptions, "name".getBytes()), Charset.defaultCharset
//        ()));
////        transaction.merge("name".getBytes(),"wy".getBytes());
////        transaction.delete("name".getBytes());
//        System.out.println(new String(transaction.get(readOptions, "name".getBytes()), Charset.defaultCharset
//        ()));
//        // 事务未提交，这个操作会被阻塞
////        transactionDB.put("name".getBytes(),"hsm1".getBytes());
//        transaction.commit();
//        transaction.close();
//        transactionDB.close();
//        repeatableRead(transactionDB);
        savePoint(transactionDB);
    }

    private static void repeatableRead(TransactionDB db) throws RocksDBException {
        ReadOptions readOptions = new ReadOptions();
        readOptions.setSnapshot(db.getSnapshot());
        WriteOptions writeOptions = new WriteOptions();
        Transaction transaction = db.beginTransaction(writeOptions);
//        transaction.put("name".getBytes(),"hsm".getBytes());
        System.out.println(new String(transaction.getForUpdate(readOptions, "name".getBytes(), true),
                Charset.defaultCharset()));
        System.out.println(new String(transaction.getForUpdate(readOptions, "name".getBytes(), true),
                Charset.defaultCharset()));
        db.releaseSnapshot(readOptions.snapshot());
    }

    /**
     * 保存点
     */
    private static void savePoint(TransactionDB db) throws RocksDBException {
        Transaction txn = db.beginTransaction(new WriteOptions());
        // 设置保存的ian
        txn.setSavePoint();
        txn.put("B".getBytes(), "b".getBytes());
        txn.rollbackToSavePoint();
        // NPE
        System.out.println(new String(txn.get(new ReadOptions(), "B".getBytes()), Charset.defaultCharset()));
        txn.commit();
    }
}
