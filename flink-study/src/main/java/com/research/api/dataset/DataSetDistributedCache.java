/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.dataset;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @fileName: DataSetDistributedCache.java
 * @description: 分布式缓存应用
 * @author: by echo huang
 * @date: 2020-02-15 15:24
 */
public class DataSetDistributedCache {

    private static final String filePath = "/Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/data/format";
    private static final String distributedCache = "distributedCache";

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //注册缓存文件,分布式执行时可以直接从缓存中拿出
        env.registerCachedFile(filePath, distributedCache);
        ArrayList<String> data = Lists.newArrayList("hello world is welcome",
                "hello world is ok",
                "hello world ok ok ok");
        DataSource<String> dataSource = env.fromCollection(data);
        dataSource.map(new MyRichMapFunction())
                .print();

    }

    static class MyRichMapFunction extends RichMapFunction<String, String> {

        @Override
        public String map(String s) throws Exception {
            return "wx:" + s;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //获取分布式缓存文件
            File file = getRuntimeContext().getDistributedCache()
                    .getFile(distributedCache);
            List<String> text = FileUtils.readLines(file);
            text.forEach(System.out::println);

        }
    }
}
