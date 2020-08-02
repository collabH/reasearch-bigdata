package com.streming.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

/**
 * @fileName: Consumer1.java
 * @description: Consumer1.java类说明
 * @author: by echo huang
 * @date: 2020-08-02 18:53
 */
public class Consumer1 {
    private static final Properties CONSUMER_PROP = new Properties();

    public static Properties buildConsumerProp() {
        CONSUMER_PROP.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-1");
        CONSUMER_PROP.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop:9092,hadoop:9093,hadoop:9094");
        CONSUMER_PROP.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        CONSUMER_PROP.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        CONSUMER_PROP.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        CONSUMER_PROP.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        CONSUMER_PROP.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer_group");
        // 最大拉取的记录数
//        CONSUMER_PROP.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
//        CONSUMER_PROP.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024 * 1024 * 10);
//        // 1s
//        CONSUMER_PROP.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1000);
//        CONSUMER_PROP.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024 * 1024 * 5);
        return CONSUMER_PROP;
    }

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(buildConsumerProp());
        ArrayList<String> topics = new ArrayList<>();
        topics.add("hello-topic");
        consumer.subscribe(topics);
        // 每1秒拉取一条消息
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println(record.key() + "----" + record.value());
                // 同步提交
                consumer.commitSync();
            }
        }
    }
}
