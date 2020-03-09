package com.reasearch.api.table;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @fileName: KafkaStreamingTable.java
 * @description: KafkaStreamingTable.java类说明
 * @author: by echo huang
 * @date: 2020-03-09 18:25
 */
public class KafkaStreamingTable {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode()
                .build();
        StreamTableEnvironment streamTable = StreamTableEnvironment.create(env, settings);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        Table result = streamTable.fromDataStream(env.addSource(new FlinkKafkaConsumer<String>("flink-kafka-topic", new SimpleStringSchema(), props))
                , "f0")
                .groupBy("f0")
                .select("f0,count(f0) as count");
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = streamTable.toRetractStream(result, Row.class);
        tuple2DataStream.filter(new FilterFunction<Tuple2<Boolean, Row>>() {
            @Override
            public boolean filter(Tuple2<Boolean, Row> value) throws Exception {
                return value.f0;
            }
        }).print();
        env.execute();
    }
}
