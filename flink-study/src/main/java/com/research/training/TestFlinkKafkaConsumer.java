/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.training;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.concurrent.atomic.LongAdder;

/**
 * @fileName: TestFlinkKafkaConsumer.java
 * @description: TestFlinkKafkaConsumer.java类说明
 * @author: by echo huang
 * @date: 2020-02-28 11:26
 */
@Slf4j
public class TestFlinkKafkaConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //启用checkpoint代替默认kafka定期向 Zookeeper 提交 offset。
        env.enableCheckpointing(500000, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("flink-kafka-topic", new SimpleStringSchema(), props);
        consumer.setCommitOffsetsOnCheckpoints(true);
        consumer.setStartFromLatest();
        DataStreamSource<String> data = env.addSource(consumer);
        //过滤存在时间并且level为E的不包含level的数据z
        data.map(
                (MapFunction<String, Tuple4<String, Long, String, Long>>) value -> {
                    String[] tokens = value.split("\t");
                    String level = tokens[2];
                    String timeStr = tokens[3];
                    long time = 0;
                    try {
                        time = DateUtils.parseDate(timeStr, "yyyy-MM-dd HH:mm:ss").getTime();
                    } catch (Exception e) {
                        log.error("日期解析异常:", e);
                    }
                    String domain = tokens[5];
                    Long traffic = Long.parseLong(tokens[6]);
                    return new Tuple4<>(level, time, domain, traffic);
                }).returns(new TypeHint<Tuple4<String, Long, String, Long>>() {
        }).filter(tuple -> tuple.f1 != 0)
                .filter(tuple -> "E".equals(tuple.f0))
                .map((MapFunction<Tuple4<String, Long, String, Long>, Tuple3<Long, String, Long>>) value -> new Tuple3<>(value.f1, value.f2, value.f3))
                .returns(new TypeHint<Tuple3<Long, String, Long>>() {
                }).assignTimestampsAndWatermarks(new PeriodicWatermarkAssigner())
                .keyBy("f1")
                .timeWindow(Time.minutes(1))
                .apply(new WindowFunction<Tuple3<Long, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<Long, String, Long>> input, Collector<Tuple3<String, String, Long>> out) throws Exception {
                        String domain = key.getField(0).toString();
                        LongAdder sum = new LongAdder();
                        input.spliterator().forEachRemaining(tuple -> sum.add(tuple.f2));
                        out.collect(new Tuple3<>(window.getStart() + "  " + window.getEnd(), domain, sum.longValue()));
                    }
                })
                .print();


        try {
            env.execute("flink kafka consumer");
        } catch (Exception e) {
            System.out.println("启动失败");
        }
    }
}
