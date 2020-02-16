///*
// * Copyright: 2020 forchange Inc. All rights reserved.
// */
//
//package com.research.api.tablesql;
//
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.TableConfig;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.api.java.BatchTableEnvironment;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//
///**
// * @fileName: QuickStart.java
// * @description: table api和sql结构
// * @author: by echo huang
// * @date: 2020-02-16 11:59
// */
//public class QuickStart {
//    public static void main(String[] args) {
//        EnvironmentSettings fsSetting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //流式查询
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, fsSetting);
//        //或者使用
//        TableEnvironment tableEnvironment = TableEnvironment.create(fsSetting);
//
//
//        //flink 批量查询
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        TableConfig tableConfig = new TableConfig();
//        BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv, tableConfig);
//
//        //blink流式查询
//        EnvironmentSettings blinkSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//
//        StreamTableEnvironment blinkTableEnv = StreamTableEnvironment.create(env, blinkSetting);
//
//
//        /**
//         * blink 批处理查询
//         */
//        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
//        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);
//
//
//    }
//}
