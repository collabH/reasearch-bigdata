/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.training;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @fileName: TestKafkaProducer.java
 * @description: kafka生产者
 * @author: by echo huang
 * @date: 2020-02-28 10:28
 */
public class TestKafkaProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        while (true) {
            StringBuilder sb = new StringBuilder();
            sb.append("org").append("\t");
            sb.append("CN").append("\t");
            sb.append(getLevels()).append("\t");
            sb.append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("\t");
            sb.append(getIps()).append("\t");
            sb.append(getDomains()).append("\t");
            sb.append(getTraffic()).append("\t");
            ProducerRecord<String, String> record = new ProducerRecord<>("flink-kafka-topic", sb.toString());
            System.out.println(String.format("data:%s", sb.toString()));
            kafkaProducer.send(record);
            Thread.sleep(500);
        }
    }

    /**
     * 获取随机数
     *
     * @return
     */
    private static long getTraffic() {
        return ThreadLocalRandom.current().nextLong(10000);
    }

    /**
     * 得到域名
     *
     * @return
     */
    private static String getDomains() {
        int i = ThreadLocalRandom.current().nextInt(4) % 4;
        return Lists.newArrayList("baidu.com", "aliyun.com", "alibaba.com", "zhang.com").get(i);
    }

    /**
     * 获得ip
     *
     * @return
     */
    private static String getIps() {
        int i = ThreadLocalRandom.current().nextInt(5) % 5;
        return Lists.newArrayList("127.0.0.1", "34.56.71.10", "23.42.56.23", "127.0.0.0", "123.0.0.0").get(i);
    }

    /**
     * 生产level数据
     *
     * @return
     */
    private static String getLevels() {
        int i = ThreadLocalRandom.current().nextInt(2) % 2;
        return Lists.newArrayList("M", "E").get(i);
    }

}
