package com.research.api.table;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import java.util.Objects;

/**
 * @fileName: TableEnvCreate.java
 * @description: table环境创建以及create table
 * @author: by echo huang
 * @date: 2020-03-09 16:27
 */
public class TableEnvCreate {


    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();


        //create flink stream table env
        EnvironmentSettings envSetting = EnvironmentSettings.newInstance().useOldPlanner()
                .withBuiltInCatalogName("stream-catalog")
                .inStreamingMode().build();
        StreamTableEnvironment streamTabEnv = StreamTableEnvironment.create(streamEnv, envSetting);

        //create table
        //创建一个临时表
        Table pojoTable = streamTabEnv.scan("X").select("xxx");
        streamTabEnv.registerTable("pojoTable", pojoTable);

        //connector
        streamTabEnv.connect(new FileSystem().path(String.valueOf(new Path(Objects.requireNonNull(TableEnvCreate.class.getClassLoader().getResource("word.txt")).getPath()))))
                .withFormat(null)
                .withSchema(new Schema())
                .inAppendMode()
                .registerTableSource("text");


        //create blink stream table env
        EnvironmentSettings envBlinkSetting = EnvironmentSettings.newInstance().useBlinkPlanner()
                .withBuiltInCatalogName("blink-stream-catalog")
                .inStreamingMode().build();
        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(streamEnv, envBlinkSetting);

        //create blink batch table env
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);

    }
}
