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
 * @fileName: TumblingWindows.java
 * @description: 滚动窗口
 * @author: by echo huang
 * @date: 2020-02-16 18:36
 */
public class TumblingWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //基于event时间特征配置,这里不能设置为event time，否则执行报错
//        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> data = environment.socketTextStream("localhost", 10001);
        //指定event time或processing time滚动窗口 依赖于setStreamTimeCharacteristic设置类型
        data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = s.split(" ");
                for (String token : tokens) {
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }).keyBy("f0")
                .timeWindow(Time.seconds(5))
                .sum("f1")
                .print();


        //按照数量来统计
      /*  data.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return "wx:" + s;
            }
        }).keyBy((KeySelector<String, String>) value -> value)
                .countWindow(3).sum(1)
                .print();*/

        environment.execute("tumbling windows");

    }
}
