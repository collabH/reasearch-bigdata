package com.research.api.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @fileName: TableIndentifier.java
 * @description: 表修饰符设置
 * @author: by echo huang
 * @date: 2020-03-09 17:55
 */
public class TableIndentifier {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        tableEnv.useCatalog("my_catalog");
        tableEnv.useDatabase("my_database");

        Table table = tableEnv.fromDataSet(env.fromElements("a", "b", "c"));
        tableEnv.registerTable("my_view", table);
        //注册一个试图叫做`exampleView`在名字为`my_catalog`的catalog里，database为`my_database`
        tableEnv.registerTable("exampleView", table);

        // register the view named 'exampleView' in the catalog named 'custom_catalog'
        // in the database named 'other_database'

        tableEnv.registerTable("other_database.exampleView", table);

        // register the view named 'View' in the catalog named 'custom_catalog' in the
        // database named 'custom_database'. 'View' is a reserved keyword and must be escaped.
        tableEnv.registerTable("`View`", table);

        // register the view named 'example.View' in the catalog named 'custom_catalog'
        // in the database named 'custom_database'
        tableEnv.registerTable("`example.View`", table);

        // register the view named 'exampleView' in the catalog named 'other_catalog'
        // in the database named 'other_database'
        tableEnv.registerTable("other_catalog.other_database.exampleView", table);
    }
}
