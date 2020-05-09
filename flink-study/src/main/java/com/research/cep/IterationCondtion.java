package com.research.cep;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Objects;
import java.util.Properties;

/**
 * @fileName: IterationCondtion.java
 * @description: cep迭代条件
 * @author: by echo huang
 * @date: 2020-04-03 17:05
 */
public class IterationCondtion {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "cep");

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<Event> dataStreamSource = environment.addSource(new FlinkKafkaConsumer<>("cep_topic", new KafkaDeserializationSchema<Event>() {


            @Override
            public boolean isEndOfStream(Event nextElement) {
                return Objects.isNull(nextElement);
            }

            @Override
            public Event deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                Gson gson = new Gson();
                String message = new String(record.value(), Charsets.UTF_8);
                return gson.fromJson(message, Event.class);
            }

            @Override
            public TypeInformation<Event> getProducedType() {
                return TypeInformation.of(Event.class);
            }
        }, props));

        //迭代条件
        Pattern<Event, Event> start = Pattern.<Event>begin("start")
                .oneOrMore()
                .where(new IterativeCondition<Event>() {
                    @Override
                    public boolean filter(Event event, Context<Event> context) throws Exception {
                        return false;
                    }
                });

        //简单条件
        start.where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return false;
            }
        });
        //将父事件通过start.subtype(subClass)转换为子事件
        start.subtype(SubEvent.class)
                .where(new SimpleCondition<SubEvent>() {
                    @Override
                    public boolean filter(SubEvent value) throws Exception {
                        return false;
                    }
                });

        //合并条件
        start.where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return false;
            }
        }).or(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return false;
            }
        }).until(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return false;
            }
        });


        // 停止条件 在循环模式（oneOrMore()和oneOrMore().optional()）的情况下，您还可以指定停止条件，例如，接受值大于5的事件，直到值的总和小于50。
//        像"(a+ until b)"（一个或多个，"a"直到"b"）的模式
//
//        一系列传入事件 "a1" "c" "a2" "b" "a3"
//
//        该库将输出结果：{a1 a2} {a1} {a2} {a3}。


    }
}
