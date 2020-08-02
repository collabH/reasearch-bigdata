package com.streming.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @fileName: AsyncProducer.java
 * @description: AsyncProducer.java类说明
 * @author: by echo huang
 * @date: 2020-08-02 17:12
 */
public class AsyncProducer {

    private static final Properties KAFKA_PROP = new Properties();

    public static Properties buildProducerProp() {
        KAFKA_PROP.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop:9092,hadoop:9093,hadoop:9094");
        KAFKA_PROP.put(ProducerConfig.ACKS_CONFIG, "all");
        // 16kb发一次
        KAFKA_PROP.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024 * 16);
        // 50ms发一次
        KAFKA_PROP.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        // 最大请求消息size
        KAFKA_PROP.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024 * 1024);
        KAFKA_PROP.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KAFKA_PROP.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KAFKA_PROP.put(ProducerConfig.RETRIES_CONFIG, 3);
        KAFKA_PROP.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "async");
        KAFKA_PROP.put(ProducerConfig.CLIENT_ID_CONFIG, "async-client");
        KAFKA_PROP.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5000);
        // send_buf大小
        KAFKA_PROP.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 5 * 1024 * 1024);
        return KAFKA_PROP;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = buildProducerProp();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<>("async-topic", "hello");

        Future<RecordMetadata> send = producer.send(record, (recordMetadata, e) -> {
            System.out.println(recordMetadata);
            if (Objects.nonNull(e)) {
                e.printStackTrace();
            }
        });
        System.out.println(send.get());

    }
}
