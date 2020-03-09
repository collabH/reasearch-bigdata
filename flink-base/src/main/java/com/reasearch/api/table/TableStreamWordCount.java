package com.reasearch.api.table;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @fileName: TableStreamWordCount.java
 * @description: TableStreamWordCount.java类说明
 * @author: by echo huang
 * @date: 2020-03-09 11:46
 */
public class TableStreamWordCount {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999);

        StreamTableEnvironment streamTabEnv = StreamTableEnvironment.create(env);
        streamTabEnv.registerDataStream("wordTab", stream, "word");

        Table result = streamTabEnv.scan("wordTab")
                .groupBy("count")
                .select("word,count(1) as count");

    }
}
