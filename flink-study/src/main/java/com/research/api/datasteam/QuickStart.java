/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.datasteam;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @fileName: QuickStart.java
 * @description: QuickStart.java类说明
 * @author: by echo huang
 * @date: 2020-02-15 17:25
 */
public class QuickStart {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost",
                9999)
                .flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (word, collector) -> {
                    String[] tokens = word.toLowerCase().split(",");
                    for (String token : tokens) {
                        collector.collect(new Tuple2<>(token, 1L));
                    }
                }).returns(new TypeHint<Tuple2<String, Long>>() {
        })
                .keyBy("f0")
                .timeWindow(Time.seconds(5))
                .sum("f1")
                .print();
        env.execute("stream executor");
    }
}
