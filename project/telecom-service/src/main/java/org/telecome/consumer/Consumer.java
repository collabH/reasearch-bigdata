package org.telecome.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.telecome.consumer.dao.HBaseDao;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @fileName: Consumer.java
 * @description: 消费kafka数据写入hbase
 * @author: by echo huang
 * @date: 2020-08-11 22:57
 */
public class Consumer {

    private static Properties consumerProp = new Properties();

    static {
        consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG, "telecom-group-id2");
        consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop:9092,hadoop:9093,hadoop:9094");
        consumerProp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProp.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 100);
        consumerProp.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        consumerProp.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 200);
        consumerProp.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024);
        consumerProp.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024 * 5);
    }

    public static void main(String[] args) throws Exception {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProp);
        kafkaConsumer.subscribe(Collections.singletonList("telecom-service"));
        HBaseDao dao = new HBaseDao();
        dao.init();
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<String, String> record : records) {
                    dao.insertData(record.value());
                }
                kafkaConsumer.commitAsync();
            }
        } finally {
            kafkaConsumer.commitSync();
        }
    }
}
