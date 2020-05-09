package com.spark.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @fileName: KafkaProducerTest.java
 * @description: KafkaProducerTest.java类说明
 * @author: by echo huang
 * @date: 2020-04-19 18:33
 */
public class KafkaProducerTest {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9093");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("client.id", "3");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

      while (true){
          producer.send(new ProducerRecord<>("replicatedTopic", "憨憨"));
          Thread.sleep(1000);
      }
    }
}
