///*
// * Copyright: 2020 forchange Inc. All rights reserved.
// */
//
//package com.research.api.tablesql;
//
//import com.google.common.collect.Lists;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.java.BatchTableEnvironment;
//import org.apache.flink.types.Row;
//
//import java.util.List;
//
///**
// * @fileName: HandleUserTable.java
// * @description: 处理csv中数据
// * @author: by echo huang
// * @date: 2020-02-16 13:38
// */
//public class HandleUserTable {
//
//    public static void main(String[] args) throws Exception {
//        // environment configuration
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
//        List<User> list = Lists.newArrayList(
//                new User(1, "hsm", 20),
//                new User(2, "zzl", 0),
//                new User(3, "lfy", 1)
//        );
//        tEnv.registerTable("Users", tEnv.fromDataSet(env.fromCollection(list)));
////        createTable(tEnv);
//
//        columnOperation(tEnv);
//    }
//
//    /**
//     * 创建表
//     *
//     * @param tableEnv
//     */
//    private static void createTable(BatchTableEnvironment tableEnv) throws Exception {
//        Table orders = tableEnv.from("Users");
//
//        Table counts = orders
//                .groupBy("id")
//                .select("id, sum(price)");
//
//        DataSet<Row> result = tableEnv.toDataSet(counts, Row.class);
//        result.print();
//
//    }
//
//    private static void columnOperation(BatchTableEnvironment tableEnv) throws Exception {
//        tableEnv.from("Users")
//                .addColumns("concat(c, 'sunny')")
//                .printSchema();
//
//    }
//}
