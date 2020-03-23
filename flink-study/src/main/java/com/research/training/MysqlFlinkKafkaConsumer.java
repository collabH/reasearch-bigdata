/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.training;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.LongAdder;

/**
 * @fileName: TestFlinkKafkaConsumer.java
 * @description: TestFlinkKafkaConsumer.java类说明
 * @author: by echo huang
 * @date: 2020-02-28 11:26
 */
@Slf4j
public class MysqlFlinkKafkaConsumer {
    public static void main(String[] args) {
        //elasticSearch sink
        ElasticsearchSink.Builder<Tuple4<String, String, String, Long>> builder = new ElasticsearchSink.Builder<>(Lists.newArrayList(new HttpHost("localhost", 9200)),
                new ElasticsearchSink2());
        builder.setBulkFlushMaxActions(1);
        //mysql source
        DataStreamSource<Map<String, String>> mysqlSource = StreamExecutionEnvironment.getExecutionEnvironment().addSource(new MySqlSource());

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //启用checkpoint代替默认kafka定期向 Zookeeper 提交 offset。
        env.enableCheckpointing(500000, CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("flink-kafka-topic", new SimpleStringSchema(), props);
        consumer.setCommitOffsetsOnCheckpoints(true);
        consumer.setStartFromLatest();
        DataStreamSource<String> data = env.addSource(consumer);
        //过滤存在时间并且level为E的不包含level的数据z
        data.map(
                (MapFunction<String, Tuple4<String, Long, String, Long>>) value -> {
                    String[] tokens = value.split("\t");
                    String level = tokens[2];
                    String timeStr = tokens[3];
                    long time = 0;
                    try {
                        time = DateUtils.parseDate(timeStr, "yyyy-MM-dd HH:mm:ss").getTime();
                    } catch (Exception e) {
                        log.error("日期解析异常:", e);
                    }
                    String domain = tokens[5];
                    Long traffic = Long.parseLong(tokens[6]);
                    return new Tuple4<>(level, time, domain, traffic);
                }).returns(new TypeHint<Tuple4<String, Long, String, Long>>() {
        }).filter(tuple -> tuple.f1 != 0)
                .filter(tuple -> "E".equals(tuple.f0))
                .map((MapFunction<Tuple4<String, Long, String, Long>, Tuple3<Long, String, Long>>) value -> new Tuple3<>(value.f1, value.f2, value.f3))
                .returns(new TypeHint<Tuple3<Long, String, Long>>() {
                }).assignTimestampsAndWatermarks(new PeriodicWatermarkAssigner())
                .keyBy("f1")
                .timeWindow(Time.minutes(1))
                .apply(new WindowFunction<Tuple3<Long, String, Long>, Tuple3<String, String, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<Long, String, Long>> input, Collector<Tuple3<String, String, Long>> out) throws Exception {
                        String domain = key.getField(0).toString();
                        LongAdder sum = new LongAdder();
                        input.spliterator().forEachRemaining(tuple -> sum.add(tuple.f2));
                        out.collect(new Tuple3<>(window.getStart() + "  " + window.getEnd(), domain, sum.longValue()));
                    }
                }).connect(mysqlSource)
                .flatMap(new CoFlatMapFunction<Tuple3<String, String, Long>, Map<String, String>, Tuple4<String, String, String, Long>>() {
                    private Map<String, String> map;

                    //log
                    @Override
                    public void flatMap1(Tuple3<String, String, Long> value, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
                        String userId = map.get(value.f1);
                        out.collect(new Tuple4<>(userId, value.f0, value.f1, value.f2));
                    }

                    //mysql
                    @Override
                    public void flatMap2(Map<String, String> value, Collector<Tuple4<String, String, String, Long>> out) throws Exception {
                        map = value;
                    }
                }).addSink(builder.build());


        try {
            env.execute("flink kafka consumer");
        } catch (Exception e) {
            System.out.println("启动失败");
        }
    }

    static class ElasticsearchSink2 implements ElasticsearchSinkFunction<Tuple4<String, String, String, Long>> {


        private IndexRequest createIndexRequest(Tuple4<String, String, String, Long> input) {
            Map<String, Object> json = new HashMap<>();
            json.put("userId", input.f0);
            json.put("time", input.f1);
            json.put("domain", input.f2);
            json.put("triffic_count", input.f3);

            return Requests.indexRequest()
                    .index("my-index2")
                    .type("_doc")
                    .source(json);
        }

        @Override
        public void process(Tuple4<String, String, String, Long> input, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            requestIndexer.add(createIndexRequest(input));
        }
    }
}
