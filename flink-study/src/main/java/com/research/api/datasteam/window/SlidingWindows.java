/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.datasteam.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @fileName: SlidingWindows.java
 * @description: SlidingWindows.java类说明
 * @author: by echo huang
 * @date: 2020-02-16 20:06
 */
public class SlidingWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("localhost", 10001);

        data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String val, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] values = val.split(" ");
                for (String value : values) {
                    collector.collect(new Tuple2<>(value, 1));
                }
            }
        }).keyBy("f0")
                //窗口大小  滑动大小  每10秒统计近5秒的wordcount
                .timeWindow(Time.seconds(5), Time.seconds(10))
                .sum("f1")
                .print();
        env.execute();
    }
}
