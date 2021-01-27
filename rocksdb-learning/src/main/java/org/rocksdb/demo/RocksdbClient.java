package org.rocksdb.demo;

import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RocksdbClient implements AutoCloseable {

    static {
        RocksDB.loadLibrary();
    }

    private Map<RocksDB, RocksIterator> rocksIteratorMap = new HashMap<>();
    private List<RocksIterator> rocksIteratorList = new ArrayList<>();

    public RocksdbClient() {
    }

    public RocksdbClient(Options options, String tableNamePath, String checkpointName,
                         int partitionCounter) throws RocksDBException {
        this.rocksIteratorList = getRocksIteratorList(options,
                getCheckPointPath(tableNamePath, checkpointName, partitionCounter));
    }


    public List<RocksIterator> getRocksIteratorList(Options options, List<String> checkPointPaths)
            throws RocksDBException {
        List<RocksIterator> rocksIteratorList = new ArrayList<>();
        for (String path : checkPointPaths) {
            rocksIteratorList.add(getRocksIterator(options, path));
        }
        return rocksIteratorList;
    }

    public RocksIterator getRocksIterator(Options options, String checkPointPath)
            throws RocksDBException {
        final RocksDB db = RocksDB.open(options, checkPointPath);
        final RocksIterator rocksIterator = db.newIterator(new ReadOptions());
        rocksIteratorMap.put(db, rocksIterator);
        return rocksIterator;
    }

    private List<String> getCheckPointPath(String tablePath, String checkpointName,
                                           int partitionCounter) {
        List<String> tablePaths = new ArrayList<>();
        partitionCounter = partitionCounter - 1;
        while (partitionCounter >= 0) {
            tablePaths.add(tablePath + "/" + partitionCounter + "/" + checkpointName);
            partitionCounter--;
        }
        return tablePaths;
    }


    @Override
    public void close() {
        for (Map.Entry<RocksDB, RocksIterator> entry : rocksIteratorMap.entrySet()) {
            entry.getValue().close();
            entry.getKey().close();
        }
    }


}