package com.streming.kafka.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.LongAdder;

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

    private static final LongAdder COUNT = new LongAdder();

    private static ThreadLocal<SimpleDateFormat> simpleDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(buildConsumerProp());
        ArrayList<String> topics = new ArrayList<>();
        topics.add("event_topic");
        consumer.subscribe(topics);
        // 每1秒拉取一条消息
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : poll) {
                JSONObject jsonObject = JSON.parseObject(record.value());
                String event = String.valueOf(jsonObject.get("event"));
                Long tiem = Long.valueOf(jsonObject.get("time").toString());
                Date date = new Date(tiem);
                String format = simpleDateFormat.get().format(date);
                if (format.contains("2020-08-06") && "ResourceSubmit".equals(event)) {
                    COUNT.increment();
                }
                // 同步提交
                consumer.commitAsync();
            }
            System.out.println("size:" + COUNT.longValue());
        }
    }
}
