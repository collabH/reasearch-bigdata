package com.streming.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @fileName: PartitionProducer.java
 * @description: PartitionProducer.java类说明
 * @author: by echo huang
 * @date: 2020-08-02 18:37
 */
public class PartitionProducer {

    public static void main(String[] args) {
        Properties properties = AsyncProducer.buildProducerProp();
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.streming.kafka.partitioner.CustomPartitioner");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //初始化事务
        producer.initTransactions();

        try {
            producer.beginTransaction();
            producer.send(new ProducerRecord<>("hello-topic", "-topic"), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println(metadata.partition() + "----" + metadata.offset());
                }
            }).get();
            producer.commitTransaction();
        } catch (Exception e) {
            producer.abortTransaction();
            e.printStackTrace();
        }


    }
}
