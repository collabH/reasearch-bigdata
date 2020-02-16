/*
 * Copyright: 2020 forchange Inc. All rights reserved.
 */

package com.research.api.datasteam.window;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @fileName: WindowFunction.java
 * @description: windowfunction test
 * @author: by echo huang
 * @date: 2020-02-16 20:32
 */
public class WindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Integer>> data = env.fromElements(new Tuple2<String, Integer>("a", 1),
                new Tuple2<String, Integer>("b", 2),
                new Tuple2<String, Integer>("a", 4));
//        foldFunction(env, data);

        //processFunction(env, data);
        reduceFunction(env, data);
    }

    private static void reduceFunction(StreamExecutionEnvironment env, DataStreamSource<Tuple2<String, Integer>> data) throws Exception {
        data.keyBy("f0")
                .timeWindow(Time.seconds(4))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
                    }
                }).print();
        env.execute();
    }

    private static void processFunction(StreamExecutionEnvironment env, DataStreamSource<Tuple2<String, Integer>> data) throws Exception {
        data.keyBy("f0")
                .timeWindow(Time.seconds(4))
                .process(new MyProcessWindowFunction()).print();
        env.execute();
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow> {

        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
            for (Tuple2<String, Integer> element : elements) {
                out.collect(context.window().getStart() + ":" + context.window().getEnd() + element.f0);
            }
        }
    }

    private static void foldFunction(StreamExecutionEnvironment env, DataStreamSource<Tuple2<String, Integer>> data) throws Exception {
        data.keyBy("f0")
                .timeWindow(Time.seconds(4))
                .fold("", new FoldFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public String fold(String s, Tuple2<String, Integer> o) throws Exception {
                        return s + o.f0;
                    }
                }).print();
        env.execute();
    }
}
