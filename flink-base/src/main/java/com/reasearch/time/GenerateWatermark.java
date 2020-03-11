package com.reasearch.time;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @fileName: GenerateWatermark.java
 * @description: 生成WaterMark的方式
 * @author: by echo huang
 * @date: 2020-03-10 16:30
 */
public class GenerateWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> data = env.addSource(new RichParallelSourceFunction<String>() {

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                //生成watermark
                //第一个为要发送的数据，第二个胃这个数据对应的时间戳
                ctx.collectWithTimestamp("data", System.currentTimeMillis());
                //直接创建一个watermark
                ctx.emitWatermark(new Watermark(System.currentTimeMillis()));
            }

            @Override
            public void cancel() {

            }
        });

        data.assignTimestampsAndWatermarks(new PeriodicWatermarkAssigner());
        data.print();

        env.execute();
    }
}
