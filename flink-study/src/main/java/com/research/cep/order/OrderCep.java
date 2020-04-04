package com.research.cep.order;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * @fileName: OrderCep.java
 * @description: OrderCep.java类说明
 * @author: by echo huang
 * @date: 2020-04-03 18:16
 */
public class OrderCep {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "order-group");
        DataStreamSource<Order> dataStreamSource = environment.addSource(new FlinkKafkaConsumer<Order>("order_cep", new KafkaDeserializationSchema<Order>() {
            @Override
            public boolean isEndOfStream(Order nextElement) {
                return Objects.isNull(nextElement);
            }

            @Override
            public Order deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                String message = new String(record.value(), Charsets.UTF_8);
                Gson gson = new Gson();
                return gson.fromJson(message, Order.class);
            }

            @Override
            public TypeInformation<Order> getProducedType() {
                return TypeInformation.of(Order.class);
            }
        }, props));

        dataStreamSource.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Order>() {
            private long delayTime = 3500;
            private long maxTime;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(maxTime - delayTime);
            }

            @Override
            public long extractTimestamp(Order element, long previousElementTimestamp) {
                long time = element.getOrderTime();
                this.maxTime = Math.max(time, this.maxTime);
                return time;
            }
        });
        KeyedStream<Order, String> keyedStream = dataStreamSource.keyBy(new KeySelector<Order, String>() {
            @Override
            public String getKey(Order value) throws Exception {
                return value.getId();
            }
        });

        Pattern<Order, Order> pattern = Pattern.<Order>begin("create")
                .where(new SimpleCondition<Order>() {
                    @Override
                    public boolean filter(Order value) throws Exception {
                        //创建订单
                        return value.getState() == 1;
                    }
                }).next("pay")
                .where(new SimpleCondition<Order>() {
                    @Override
                    public boolean filter(Order value) throws Exception {
                        return value.getState() == 2;
                    }
                })
                .next("pay_fail")
                .where(new SimpleCondition<Order>() {
                    @Override
                    public boolean filter(Order value) throws Exception {
                        return value.getState() == 4;
                    }
                }).times(3).within(Time.seconds(20));

        PatternStream<Order> patternStream = CEP.pattern(keyedStream, pattern);

        DataStream<String> select = patternStream.select(new PatternSelectFunction<Order, String>() {
            @Override
            public String select(Map<String, List<Order>> map) throws Exception {
                System.out.println(map.get("pay_fail"));
                System.out.println(map.get("create"));
                System.out.println(map.get("pay"));
                return "订单支付超时三次";
            }
        });

        select.print();

        environment.execute();
    }
}
