/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.connector.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @fileName: FlinkKafkaProducerTest.java
 * @description: FlinkKafkaProducerTest.java类说明
 * @author: by echo huang
 * @date: 2020-02-23 12:54
 */
public class FlinkKafkaProducerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>("flink-kafka-topic", new SimpleStringSchema(), properties);
        data.addSink(producer);
        env.execute("kafka flink producer");
    }
}
