/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.core;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import scala.Tuple2;

import java.io.IOException;

/**
 * @fileName: CoreTest.java
 * @description: CoreTest.java类说明
 * @author: by echo huang
 * @date: 2020-02-13 22:06
 */
public class CoreTest {
    //获取执行换成
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    /**
     * 执行步骤
     */
    public void executorStep() throws Exception {
        //创建本地环境
        env = ExecutionEnvironment.createLocalEnvironment(1);
        //远程环境搭建
        env = ExecutionEnvironment.createRemoteEnvironment("localhost", 100, "jar");

        //加载/创建初始数据
        DataSource<String> dataSource = env.readTextFile("file:///text.txt");

        //指定数据转换操作
        MapOperator<String, Object> map = dataSource.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String s) throws Exception {
                return null;
            }
        });
        //sink
        map.writeAsText("/dsad");

        //执行
        JobExecutionResult execute = env.execute();

    }

    /**
     * 指定键
     */
    public void theSpecifiedKey() {
        DataSource<Tuple2<String, Long>> dataSource = env.readFile(new FileInputFormat<Tuple2<String, Long>>() {
            @Override
            public boolean reachedEnd() throws IOException {
                return false;
            }

            @Override
            public Tuple2<String, Long> nextRecord(Tuple2<String, Long> stringLongTuple2) throws IOException {
                return null;
            }
        }, "tt");
        //定义tuple2的String作为键来分组
        dataSource.groupBy(0);
        //指定tuple2的String，Long字段来分组
        dataSource.groupBy(0, 1);
    }


    /**
     * 函数转换
     */
    class MyFuntion implements MapFunction<String, String> {

        @Override
        public String map(String s) throws Exception {
            return null;
        }
    }

    /**
     * 富函数
     */
    class MyRiskFunction extends RichMapFunction<String, String> {

        @Override
        public String map(String s) throws Exception {
            return null;
        }

        @Override
        public void setRuntimeContext(RuntimeContext t) {
            super.setRuntimeContext(t);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return super.getRuntimeContext();
        }

        @Override
        public IterationRuntimeContext getIterationRuntimeContext() {
            return super.getIterationRuntimeContext();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }
    }

    public void functionTransform() {
        DataSource<String> dataSource = env.readTextFile("tt");
        //匿名内部类
        dataSource.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String s) throws Exception {
                return null;
            }
        });
        //自定义类
        dataSource.map(new MyFuntion());
        //lambda方式
        dataSource.map(val -> null);
    }

}
