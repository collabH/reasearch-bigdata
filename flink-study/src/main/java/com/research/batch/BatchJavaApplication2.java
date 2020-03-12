/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.batch;

import com.google.common.base.Stopwatch;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @fileName: BatchJavaApplication.java
 * @description: Java批处理应用程序
 * @author: by echo huang
 * @date: 2020-02-12 17:16
 */
public class BatchJavaApplication2 {
    public static void main(String[] args) throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();
        //step1: 准备基础环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> strings = new ArrayList<>();
        strings.add("hello world zs s1");
        strings.add("hello world zs s1");
        strings.add("hello world zs s13");
        strings.add("hello world zs s13");
        strings.add("hello1 world zs s13");
        strings.add("hello1 world zs s13");
        strings.add("hello1 world zs s13");
        strings.add("hello world zs s13");
        strings.add("hello world zs s13");
        strings.add("hello world2 zs s2");
        strings.add("hello world2 zs s2");
        strings.add("hello world2 zs s2");
        strings.add("hello world2 zs s2");
        strings.add("hello world zs s2");
        strings.add("hello world zs1 s2");
        strings.add("hello world zs1 s2");
        strings.add("hello world zs1 s2");
        strings.add("hello world zs s1");
        //step2:读取文件 url:URI (e.g., "file:///some/local/file" or "hdfs://host:port/file/path"). 代表数据源
        DataSource<String> text = environment.fromCollection(strings);
        System.out.println(text.getType().getTypeClass());

        //step3:转换数据，数据处理
        text.filter(Objects::nonNull)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] tokens = line.toLowerCase().split(" ");
                        if (tokens.length > 0) {
                            for (String token : tokens) {
                                collector.collect(new Tuple2<>(token, 1));
                            }
                        }
                    }
                }).groupBy(0).sum(1).print();
        //   environment.execute();
        System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
    }
}
