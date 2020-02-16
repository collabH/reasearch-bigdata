/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.datasteam;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @fileName: Transformations.java
 * @description: 算子操作
 * @author: by echo huang
 * @date: 2020-02-15 18:11
 */
public class Transformations {
    /**
     * map函数，传入数据流输出数据流，对一个数据进行处理
     *
     * @param env
     * @throws Exception
     */
    private static void mapFunction(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> data = env.fromElements("A", "B", "C");
        data.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return "wx:" + s;
            }
        }).print();
        env.execute();
    }

    /**
     * 一个数据输出一个或多个数据
     *
     * @param env
     * @throws Exception
     */
    private static void flatMapFunction(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> data = env.fromElements("A s", "s B", "ds C");
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(s2);
                }
            }
        }).print();
        env.execute();
    }

    /**
     * 过滤保留返回为true的数据
     *
     * @param env
     * @throws Exception
     */
    private static void filterMapFunction(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> data = env.fromElements("A", "B", "C");
        data.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return "A".equals(s);
            }
        }).print();
        env.execute();
    }

    /**
     * 根据可以来分区
     * 注意点
     * 1.pojo类型但是不能重写hashcode()方法和需要依赖Objects.hashCode()实现
     * 2.它是一个任何类型的数据
     * DataStream->KeyedStream
     *
     * @param env
     * @throws Exception
     */
    private static void keyBy(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<String> data = env.fromElements("A", "B", "C");
        data.keyBy(0).print();
        env.execute();
    }

    /**
     * reduce操作:根据key来分区处理这个key分区的元素之间的reduce操作
     * KeyedStream->DataStream
     *
     * @param env
     * @throws Exception
     */
    private static void reduce(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Tuple2<String, Integer>> data = env.fromElements(new Tuple2<String, Integer>("A", 2),
                new Tuple2<String, Integer>("B", 2),
                new Tuple2<String, Integer>("C", 1),
                new Tuple2<String, Integer>("A", 2),
                new Tuple2<String, Integer>("C", 1),
                new Tuple2<String, Integer>("B", 1));
        data.keyBy("f0")
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                }).print();
        env.execute();
    }

    /**
     * 扰动函数
     *
     * @param env
     * @throws Exception
     */
    private static void shuffle(StreamExecutionEnvironment env) throws Exception {
        env.fromElements(1, 2, 3, 4, 5, 6)
                .shuffle().print();
        env.execute();
    }

    /**
     * 聚合函数
     *
     * @param env
     * @throws Exception
     */
    private static void aggFunction(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Integer> data = env.fromElements(1, 2, 3, 4, 5, 6, 123);
        KeyedStream<Integer, Integer> keyedStream = data.keyBy(i -> i);
        keyedStream.sum(0);
        keyedStream.max(0);
        keyedStream.min(0);

    }

    /**
     * 可以在已经分区的KeyedStreams上定义Windows。Windows根据一些特征(例如，最近5秒内到达的数据)对每个密钥中的数据进行分组。
     *
     * @param env
     * @throws Exception
     */
    private static void windowFunction(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Integer> data = env.fromElements(1, 2, 3, 4, 5, 6, 123);
        KeyedStream<Integer, Integer> keyedStream = data.keyBy(i -> i);
        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
    }

    /**
     * 求俩个dataStream的并集
     *
     * @param env
     * @throws Exception
     */
    private static void unionFunction(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Integer> data1 = env.fromCollection(Lists.newArrayList(1, 2, 3, 4));
        DataStreamSource<Integer> data2 = env.fromCollection(Lists.newArrayList(5, 6, 7));

        data1.union(data2)
                .print();
    }

    /**
     * join类似sql
     *
     * @param env
     * @throws Exception
     */
    private static void joinFunction(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Tuple2<String, Integer>> data1 = env.fromCollection(Lists.newArrayList(new Tuple2<>("A", 1),
                new Tuple2<>("B", 2)));
        DataStreamSource<Tuple2<String, Integer>> data2 = env.fromCollection(Lists.newArrayList(new Tuple2<>("A", 3),
                new Tuple2<>("B", 4)));

        data1.join(data2)
                .where((KeySelector<Tuple2<String, Integer>, String>) stringIntegerTuple2 -> stringIntegerTuple2.f0)
                .equalTo((KeySelector<Tuple2<String, Integer>, String>) stringIntegerTuple2 -> stringIntegerTuple2.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .apply((JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Integer>) (stringIntegerTuple2, stringIntegerTuple22) -> stringIntegerTuple2.f1 + stringIntegerTuple22.f1).print();
    }

    /**
     * @param env
     * @throws Exception
     */
    private static void connectFunction(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Tuple2<String, Integer>> data1 = env.fromCollection(Lists.newArrayList(new Tuple2<>("A", 1),
                new Tuple2<>("B", 2)));
        DataStreamSource<Tuple2<String, Integer>> data2 = env.fromCollection(Lists.newArrayList(new Tuple2<>("A", 3),
                new Tuple2<>("B", 4)));
        data1.connect(data2)
                .keyBy("f0", "f0")
                .map(new CoMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object map1(Tuple2<String, Integer> value) throws Exception {
                        return value.f1;
                    }

                    @Override
                    public Object map2(Tuple2<String, Integer> value) throws Exception {
                        return value.f1;
                    }
                }).print();


    }

    private static void splitFunction(StreamExecutionEnvironment env) throws Exception {
        SplitStream<String> split = env.fromElements("a", "b", "c", "e", "g", "hello")
                .split(value -> {
                    List<String> list = Lists.newArrayList();
                    if ("hello".equals(value)) {
                        list.add("happy");
                    } else {
                        list.add("unhappy");
                    }
                    return list;
                });
        //select算子
        split.select("happy").print();
    }

    /**
     * 选择一个tuple的属性的子集合
     *
     * @param env
     * @throws Exception
     */
    private static void project(StreamExecutionEnvironment env) throws Exception {
        DataStreamSource<Tuple2<String, Integer>> data1 = env.fromCollection(Lists.newArrayList(new Tuple2<>("A", 1),
                new Tuple2<>("B", 2)));

        data1.project(1).print();
    }


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        mapFunction(env);
//        flatMapFunction(env);
//        filterMapFunction(env);
//        reduce(env);
//        shuffle(env);
//        unionFunction(env);
//        joinFunction(env);
//        connectFunction(env);
//        splitFunction(env);
        project(env);
        env.execute();
    }
}
