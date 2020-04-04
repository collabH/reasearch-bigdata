package com.research.cep.order;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @fileName: OrderMessageProducer.java
 * @description: OrderMessageProducer.java类说明
 * @author: by echo huang
 * @date: 2020-04-04 12:10
 */
public class OrderMessageProducer {
    static String[] orderId = new String[]{"order1", "order2", "order3", "order4", "order5"};

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        int state = 1;
        int count = 1;
        Gson gson = new Gson();
        while (true) {
            if (state == 3 && count % 2 == 0) {
                state = 4;
            }
            Order order = new Order();
            order.setId("orderId" + count);
            order.setOrderTime(System.currentTimeMillis());
            order.setState(state);
            state++;
            if (state == 5) {
                state = 1;
                count++;
            }
            String message = gson.toJson(order);
            System.out.println(message);
            kafkaProducer.send(new ProducerRecord<>("order_cep", message));
            Thread.sleep(1000);

        }
    }
}
