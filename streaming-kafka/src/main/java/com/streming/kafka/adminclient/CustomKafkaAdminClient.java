package com.streming.kafka.adminclient;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @fileName: CustomKafkaAdminClient.java
 * @description: CustomKafkaAdminClient.java类说明
 * @author: by echo huang
 * @date: 2020/10/3 11:59 下午
 */
public class CustomKafkaAdminClient {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String broker = "hadoop:9092";
        String topic = "topic-admin";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);

        AdminClient adminClient = AdminClient.create(props);
        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
        ArrayList<NewTopic> objects = new ArrayList<>();
        objects.add(newTopic);
        // 创建topic
        createTopic(objects, adminClient);
    }

    /**
     * 创建topic
     */
    public static void createTopic(List<NewTopic> topics, AdminClient client) throws ExecutionException, InterruptedException {
        CreateTopicsResult createTopicsResult = client.createTopics(topics, new CreateTopicsOptions());
        System.out.println(createTopicsResult.all().get());
        client.close();
    }
}
