/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.streaming;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @fileName: StreamingApplication.java
 * @description: wc统计的数据来源于socket
 * @author: by echo huang
 * @date: 2020-02-12 18:14
 */
public class StreamingApplication3 {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class WordCount {
        private String word;
        private Integer count;
    }

    public static void main(String[] args) throws Exception {
        //step1:设置流失处理环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //step2:读取文件
        DataStreamSource<String> text = env.socketTextStream("localhost", 10001);
        //step3:数据转换处理
        text.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String word, Collector<WordCount> collector) throws Exception {
                String[] tokens = word.toLowerCase().split(" ");
                for (String token : tokens) {
                    collector.collect(new WordCount(token, 1));
                }
            }
        }).keyBy(WordCount::getWord).timeWindow(Time.seconds(5))
                .sum("count")
                .setParallelism(1)
                .print();
        env.execute();
    }
}
