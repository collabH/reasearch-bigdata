package com.reasearch.api.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Objects;

/**
 * @fileName: TableWordCount.java
 * @description: table api wordcount demo
 * @author: by echo huang
 * @date: 2020-03-09 11:24
 */
public class TableWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        //获取resource目录下数据
        String wordPath = Objects.requireNonNull(TableWordCount.class.getClassLoader().getResource("word.txt")).getPath();
        //注册数据源
        tableEnv.connect(new FileSystem().path(wordPath))
                .withFormat(new OldCsv().field("word", Types.STRING).lineDelimiter(","))
                .withSchema(new Schema().field("word", Types.STRING))
                .registerTableSource("fileSource");

        //扫描数据源做对应操作
        Table resultTab = tableEnv.scan("fileSource")
                .groupBy("word")
                .select("word,count(1) as count");

        //转换为dataset并且打印
        tableEnv.toDataSet(resultTab, Row.class)
                .print();

    }
}
