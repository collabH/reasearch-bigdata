/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.connector.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

/**
 * @fileName: FlinkKafkaConsumerTest.java
 * @description: FlinkKafkaConsumerTest.java类说明
 * @author: by echo huang
 * @date: 2020-02-22 20:53
 */
public class FlinkKafkaConsumerTest {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //启用checkpoint代替默认kafka定期向 Zookeeper 提交 offset。
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.addSource(new FlinkKafkaConsumer<String>("flink-kafka-topic", new SimpleStringSchema(), properties))
                .print();
//        useDeserializationSchema(env, properties);
        env.execute("flink kafka job");
    }

    /**
     * 使用json序列化方式
     *
     * @param env
     * @param properties
     */
    private static void useDeserializationSchema(StreamExecutionEnvironment env, Properties properties) {
        FlinkKafkaConsumer<ObjectNode> myConsumer = new FlinkKafkaConsumer<>("flink-kafka-topic", new JSONKeyValueDeserializationSchema(true), properties);
        myConsumer.setStartFromEarliest();     // 尽可能从最早的记录开始
        myConsumer.setStartFromLatest();       // 从最新的记录开始
        myConsumer.setStartFromTimestamp(10000); // 从指定的时间开始（毫秒）
        myConsumer.setStartFromGroupOffsets(); // 默认的方法
        env.addSource(myConsumer)
                .print();
    }


}
