package com.spark.kafka;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Properties;

/**
 * @fileName: KafkaConsumerTest.java
 * @description: KafkaConsumerTest.java类说明
 * @author: by echo huang
 * @date: 2020-04-19 18:39
 */
public class KafkaConsumerTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9093");
        properties.setProperty("group.id", "test-group-id");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Lists.newArrayList("replicatedTopic"));
        System.out.println(consumer.poll(Duration.ZERO));
    }
}
