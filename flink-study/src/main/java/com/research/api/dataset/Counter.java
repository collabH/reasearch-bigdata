/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.dataset;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @fileName: Counter.java
 * @description: Counter.java类说明
 * @author: by echo huang
 * @date: 2020-02-14 21:46
 */
public class Counter {
    private static final String filePath = "file:///Users/babywang/Documents/reserch/dev/workspace/reasech-bigdata/data/format";

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        AtomicLong counter = new AtomicLong(0);
        List<String> list = Lists.newArrayList("A", "N", "C", "D");
        //设置多并行流是就无法统计
       /* env.fromCollection(list)
                .map(new RichMapFunction<String, Long>() {
                    @Override
                    public Long map(String s) throws Exception {
                        return counter.incrementAndGet();
                    }
                }).setParallelism(4).print();*/
       //利用富函数的计数器传递参数方式来完成
        env.fromCollection(list)
                .map(new MyRichMap()).setParallelism(1).writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);
        Object dataSetCounter = env.execute("Counter").getAccumulatorResult("write-counter");
        System.out.println(dataSetCounter);
    }

    static class MyRichMap extends RichMapFunction<String, String> {
        //设置计数器
        private LongCounter counter = new LongCounter(0);

        @Override
        public String map(String s) throws Exception {
            counter.add(1);
            return s;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //定义计数器
            getRuntimeContext().addAccumulator("write-counter", counter);
        }
    }
}
