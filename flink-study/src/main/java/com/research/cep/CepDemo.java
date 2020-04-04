package com.research.cep;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @fileName: CepDemo.java
 * @description: CepDemo.java类说明
 * @author: by echo huang
 * @date: 2020-04-03 10:46
 */
public class CepDemo {
    public static void main(String[] args) throws Exception {
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
        dataStreamSource.assignTimestampsAndWatermarks(new Watermarks());
//        OutputTag<String> outputTag = new OutputTag<>("timeout");

        //定义cep模式
        Pattern<Event, Event> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return StringUtils.equals(value.getState(), "start");
            }
        }).next("sign")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.getSign() == 10;
                    }
                }).followedBy("end")
                .where(
                        new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return StringUtils.equals(value.getName(), "end");
                            }
                        }
                ).within(Time.seconds(10));

        PatternStream<Event> patternStream = CEP.pattern(dataStreamSource, pattern);

        SingleOutputStreamOperator<String> mainStream = patternStream.process(new PatternProcessFunction<Event, String>() {
            @Override
            public void processMatch(Map<String, List<Event>> map, Context context, Collector<String> collector) throws Exception {
                collector.collect(map.values().stream()
                        .flatMap(events -> events.stream().map(Event::toString)).collect(Collectors.joining(",")));
            }
        });

        mainStream.print();

        //超时处理
       /* DataStream<String> timeOutStream = patternStream.flatSelect(outputTag, new PatternFlatTimeoutFunction<Event, String>() {
            //超时数据处理
            @Override
            public void timeout(Map<String, List<Event>> map, long l, Collector<String> collector) throws Exception {
                String message = map.values().stream()
                        .flatMap(events -> events.stream()
                                .map(Event::toString))
                        .findFirst().orElse("");
                collector.collect(message);
            }
        }, new PatternFlatSelectFunction<Event, String>() {
            //正常数据出炉
            @Override
            public void flatSelect(Map<String, List<Event>> map, Collector<String> collector) throws Exception {
                String message = map.values().stream()
                        .flatMap(events -> events.stream()
                                .map(Event::toString))
                        .findFirst().orElse("");
                collector.collect(message);
            }
        }).getSideOutput(outputTag);*/
//        timeOutStream.print();


        environment.execute();
    }
}
