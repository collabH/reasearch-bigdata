/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.dataset;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;

/**
 * @fileName: DataSetSinks.java
 * @description: sink输出
 * @author: by echo huang
 * @date: 2020-02-14 21:14
 */
public class DataSetSinks {
    private static final String filePath = "file:///Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/data/format";
    private static final String csvPath = "file:///Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/data/csv";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        writeTextSink(env);
//        writeCsvText(env);
        writeAsFormattedText(env);
    }

    private static void writeTextSink(ExecutionEnvironment env) throws Exception {
        ArrayList<Integer> list = Lists.newArrayList(1, 23, 32144, 512);
        //env.fromCollection(list).writeAsText(filePath);
        //开启文件覆盖，不设置并行度就写入文件，设置并行度会写到文件夹下的多个文件里
        env.fromCollection(list).writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(4);
        JobExecutionResult result = env.execute("writeTextSink");
        System.out.println("执行时间" + result.getNetRuntime());

    }

    private static void writeCsvText(ExecutionEnvironment env) throws Exception {
        ArrayList<Tuple1<Integer>> list = Lists.newArrayList(new Tuple1<>(1), new Tuple1<>(1213), new Tuple1<>(23), new Tuple1<>(134));
        env.fromCollection(list).
                writeAsCsv(csvPath, ",", ",");
        JobExecutionResult result = env.execute("writeCsvText");
        System.out.println("执行时间" + result.getNetRuntime());

    }

    private static void writeAsFormattedText(ExecutionEnvironment env) throws Exception {
        ArrayList<Integer> list = Lists.newArrayList(1, -1, -32144, -512);
        env.fromCollection(list)
                .writeAsFormattedText(filePath, FileSystem.WriteMode.OVERWRITE, (TextOutputFormat.TextFormatter<Integer>) value -> "zzl 智商" + value);
        JobExecutionResult result = env.execute();
        System.out.println("执行时间" + result.getNetRuntime());

    }
}
