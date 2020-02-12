/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * @fileName: StreamingApplication.java
 * @description: wc统计的数据来源于socket
 * @author: by echo huang
 * @date: 2020-02-12 18:14
 */
public class StreamingApplication {

    public static void main(String[] args) throws Exception {
        //step1:设置流失处理环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //step2:读取文件
        DataStreamSource<String> text = env.socketTextStream("localhost", 10001);
        //step3:数据转换处理
        text.filter(Objects::nonNull)
                .setParallelism(1)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String word, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] tokens = word.toLowerCase().split(" ");
                        for (String token : tokens) {
                            collector.collect(new Tuple2<>(token, 1));
                        }
                    }
                }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1)
                .print();
        env.execute();
    }
}
