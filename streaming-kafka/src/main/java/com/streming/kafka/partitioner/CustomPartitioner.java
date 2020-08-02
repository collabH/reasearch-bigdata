package com.streming.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @fileName: CustomPartitioner.java
 * @description: 自定义分区器
 * @author: by echo huang
 * @date: 2020-08-02 18:25
 */
public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        System.out.println(cluster.partitionCountForTopic("hello-topic"));
        if (String.valueOf(value).contains("hello")) {
            return 0;
        }
        return 1;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
