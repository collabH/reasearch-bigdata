package com.reasearch.api.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * @fileName: TableEmit.java
 * @description: TableEmit.java类说明
 * @author: by echo huang
 * @date: 2020-03-09 18:16
 */
public class TableEmit {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(env);
        Schema schema = new Schema()
                .field("a", Types.INT)
                .field("b", Types.STRING)
                .field("c", Types.LONG);

        batchTableEnvironment.connect(new FileSystem()
                .path("/path/hello/word.txt"))
                .withFormat(new OldCsv().lineDelimiter(","))
                .withSchema(schema)
                .registerTableSink("csvSink");

        batchTableEnvironment.scan("csvSink")
                .insertInto("CsvSinkTable");

    }
}
